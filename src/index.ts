/**
 * AgentLink — Cloudflare Workers + Durable Objects
 * WebSocket relay/signaling server untuk komunikasi antar Claude agent.
 *
 * Endpoints:
 *   GET  /                        → halaman dashboard (Static Assets)
 *   GET  /info                    → info server
 *   POST /admin/prune-rooms       → buang room warisan mode publik (butuh REGISTRY_ADMIN_TOKEN)
 *   GET  /rooms                   → statistik aktivitas agregat (tanpa ID/nama room)
 *   WS   /connect/:room_id        → join room (query: ?name=&token=, token wajib)
 *   POST /rooms                   → buat room baru (body: {name})
 *
 * Semua room bersifat private: join selalu memerlukan token yang dibagikan owner.
 */

import { DurableObject } from "cloudflare:workers";

import {
  applyCapabilityProfilePatch,
  buildCapabilitySkillIndex,
  CAPABILITY_AVAILABILITY_VALUES,
  createCapabilityRegistryManifest,
  listCapabilityProfiles,
  removeCapabilityProfile,
  ROOM_CAPABILITY_REGISTRY_KEY,
  upsertCapabilityProfile,
  validateCapabilityProfilePatch,
  type CapabilityPresenceOverlay,
  type CapabilityRegistryManifest,
} from "./capability-registry";
import { createAck, createRoomEvent, createRoomMessage } from "./message-protocol";
import {
  buildHeartbeatConfig,
  shouldCheckpointPresence,
  shouldHydrateActiveSessions,
  shouldPruneSessionCheckpoint,
  toHydratedPresenceState,
  shouldResumeSession,
  toPresenceSnapshot,
  TRANSIENT_CHECKPOINT_BATCH_DELAY_SECONDS,
  type AgentPresenceSnapshot,
  type StoredRoomSession,
} from "./presence";
import { isUnjoinableRoom, summarizeRoomActivity, type StoredRoomMeta } from "./room-meta";
import {
  grantRoomAdmin,
  normalizeRoomRoleState,
  resolveRoomRoleLabel,
  revokeRoomAdmin,
  type RoomRoleLabel,
  type StoredRoomRoleState,
} from "./room-roles";
import {
  acceptDelegationOffer,
  createDelegationOffer,
  createTaskRegistryManifest,
  deferDelegationOffer,
  getTask,
  listTasks,
  rejectDelegationOffer,
  ROOM_TASK_REGISTRY_KEY,
  type StoredTaskManifest,
  type TaskPriority,
  type TaskRegistryManifest,
} from "./task-registry";

// ─── Types ────────────────────────────────────────────────────────────────────

export interface Env {
  AGENTLINK_ROOM: DurableObjectNamespace<AgentLinkRoom>;
  AGENTLINK_REGISTRY: DurableObjectNamespace<AgentLinkRegistry>;
  ASSETS: Fetcher;
  /**
   * Secret opsional untuk endpoint pemeliharaan `/admin/*`.
   * Bila tidak diset, endpoint admin dimatikan sepenuhnya.
   */
  REGISTRY_ADMIN_TOKEN?: string;
}

interface RoomMeta extends StoredRoomMeta {
  room_id: string;
  name: string;
  token: string;
  created_at: string;
  agent_count: number;
}

interface AgentInfo {
  agent_id: string;
  stable_agent_identity_id?: string;
  name: string;
  joined_at: string;
  last_seen_at: string;
  presence: "online" | "offline";
  role_label: RoomRoleLabel;
}

interface TaskEventPayload {
  task_id: string;
  title: string;
  status: string;
  offer_state: string;
  acceptance_state: string;
  delegated_by: string;
  delegated_by_identity_id?: string;
  offered_to_agent_id: string;
  offered_to_identity_id?: string;
  responsible_agent_id: string | null;
  responsible_identity_id?: string;
  point_of_contact_agent_id: string;
  point_of_contact_identity_id?: string;
  priority: TaskPriority;
  response_reason: string | null;
  deferred_until: string | null;
  lease_until: string | null;
  updated_at: string;
}

interface RoomRoleAckPayload {
  owner_stable_identity_id?: string;
  admin_stable_identity_ids: string[];
  role_label: RoomRoleLabel;
  target_agent_id: string;
  target_stable_identity_id: string;
  target_role_label: RoomRoleLabel;
}

interface AgentSessionState extends StoredRoomSession {
  room_id: string;
}

interface WsMessage {
  type:
    | "send"
    | "broadcast"
    | "ping"
    | "pong"
    | "message"
    | "event"
    | "info"
    | "task_offer"
    | "task_accept"
    | "task_reject"
    | "task_defer"
    | "capability_upsert"
    | "capability_set_availability"
    | "capability_remove"
    | "room_admin_add"
    | "room_admin_remove";
  [key: string]: unknown;
}

interface RoomSessionCheckpointManifest {
  updated_at: string;
  sessions: Record<string, StoredRoomSession>;
}

const ROOM_SESSION_CHECKPOINTS_KEY = "room:session-checkpoints";
/**
 * Room DO tidak tahu ID-nya sendiri di luar konteks request, padahal alarm perlu
 * tahu room mana yang dilaporkan — termasuk saat agent terakhir sudah keluar dan
 * tidak ada WebSocket tersisa untuk dibaca.
 */
const ROOM_ID_KEY = "room:id";

// ─── Worker Entry ─────────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // CORS headers
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-Room-Token",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    if (request.method === "GET" && path === "/health") {
      return Response.json({ status: "ok", service: "AgentLink" }, {
        headers: { ...corsHeaders, "Cache-Control": "public, max-age=60" },
      });
    }

    if (request.method === "GET" && path === "/sitemap.xml") {
      return new Response(buildSitemap(url.origin), {
        headers: { "Content-Type": "application/xml; charset=utf-8", "Cache-Control": "public, max-age=3600" },
      });
    }

    if (request.method === "GET" && path === "/robots.txt") {
      return new Response([
        "User-agent: *",
        "Allow: /",
        "Content-Signal: ai-train=no, search=yes, ai-input=yes",
        `Sitemap: ${url.origin}/sitemap.xml`,
        `Agentmap: ${url.origin}/.well-known/ai-catalog.json`,
        "",
      ].join("\n"), {
        headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "public, max-age=3600" },
      });
    }

    const discoveryResponse = buildDiscoveryResponse(path, url.origin);
    if (request.method === "GET" && discoveryResponse) {
      return discoveryResponse;
    }

    if (request.method === "GET" && path === "/auth.md") {
      return markdownResponse(buildAuthMarkdown(url.origin));
    }

    if (request.method === "GET" && path === "/" && acceptsMarkdown(request)) {
      return markdownResponse(buildHomepageMarkdown(url.origin));
    }

    if (request.method === "GET" && path === "/") {
      const assetResponse = await env.ASSETS.fetch(request);
      return withDiscoveryLinks(assetResponse);
    }

    // ── GET /dashboard ── halaman dashboard sekarang di root; jaga link lama.
    if ((path === "/dashboard" || path === "/dashboard/") && request.method === "GET") {
      return Response.redirect(new URL("/", url).toString(), 301);
    }

    // ── GET /info ── info server.
    // Root "/" dilayani Static Assets (halaman dashboard), jadi info pindah ke sini.
    if (path === "/info" && request.method === "GET") {
      return Response.json({
        name: "AgentLink",
        version: "3.0.1",
        backend: "Cloudflare Workers + Durable Objects",
        endpoints: {
          dashboard: "GET /",
          server_info: "GET /info",
          room_activity_stats: "GET /rooms",
          create_room: "POST /rooms",
          connect: "WS /connect/:room_id?name=<name>&token=<token>",
          capability_agents: "GET /capabilities/:room_id/agents?token=<token>",
          capability_agent: "GET /capabilities/:room_id/agents/:agent_id?token=<token>",
          capability_skills: "GET /capabilities/:room_id/skills?token=<token>",
          capability_skill: "GET /capabilities/:room_id/skills/:skill_id?token=<token>",
          task_list: "GET /tasks/:room_id?token=<token>",
          task_get: "GET /tasks/:room_id/:task_id?token=<token>",
        },
      }, { headers: corsHeaders });
    }

    // ── POST /admin/prune-rooms ── buang room warisan mode publik.
    // Mati total bila REGISTRY_ADMIN_TOKEN tidak diset, dan dry-run kecuali
    // confirm=true. Yang bisa dihapus hanya room yang memang sudah ditolak
    // registry, jadi tidak ada room hidup yang bisa jadi korban.
    if (path === "/admin/prune-rooms" && request.method === "POST") {
      const adminToken = env.REGISTRY_ADMIN_TOKEN;
      if (!adminToken) {
        return Response.json({
          success: false,
          error: "Admin endpoint is disabled: REGISTRY_ADMIN_TOKEN is not set.",
        }, { status: 404, headers: corsHeaders });
      }
      if (!timingSafeEqual(request.headers.get("X-Admin-Token") || "", adminToken)) {
        return Response.json(
          { success: false, error: "Unauthorized." },
          { status: 403, headers: corsHeaders },
        );
      }

      const registry = env.AGENTLINK_REGISTRY.get(
        env.AGENTLINK_REGISTRY.idFromName("global"),
      );
      const confirm = url.searchParams.get("confirm") === "true";
      const resp = await registry.fetch(new Request(
        `http://internal/prune-unjoinable?confirm=${confirm}`,
        { method: "POST" },
      ));
      return new Response(await resp.text(), {
        status: resp.status,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // ── GET /rooms ── statistik agregat; tidak pernah membocorkan ID/nama room
    if (path === "/rooms" && request.method === "GET") {
      const registry = env.AGENTLINK_REGISTRY.get(
        env.AGENTLINK_REGISTRY.idFromName("global")
      );
      const resp = await registry.fetch(new Request("http://internal/list"));
      const stats = await resp.json() as ReturnType<typeof summarizeRoomActivity>;
      return Response.json(stats, { headers: corsHeaders });
    }

    // ── POST /rooms ── buat room baru
    if (path === "/rooms" && request.method === "POST") {
      let body: {
        name?: string;
        owner_stable_identity_id?: string;
      } = {};
      try {
        body = await request.json();
      } catch {}

      const name = (body.name || "unnamed").slice(0, 50);
      const ownerStableIdentityId = sanitizeStableAgentIdentityId(
        typeof body.owner_stable_identity_id === "string"
          ? body.owner_stable_identity_id
          : null,
      );
      if (!ownerStableIdentityId) {
        return Response.json({
          success: false,
          error: "owner_stable_identity_id is required to create a room.",
        }, { status: 400, headers: corsHeaders });
      }
      const room_id = generateId(6);
      const token = generateId(12);
      const created_at = new Date().toISOString();

      // Simpan ke registry
      const registry = env.AGENTLINK_REGISTRY.get(
        env.AGENTLINK_REGISTRY.idFromName("global")
      );
      await registry.fetch(new Request("http://internal/register", {
        method: "POST",
        body: JSON.stringify({
          room_id,
          name,
          token,
          created_at,
          owner_stable_identity_id: ownerStableIdentityId,
          admin_stable_identity_ids: [],
        }),
      }));

      return Response.json({
        success: true,
        room_id,
        name,
        token,
        owner_stable_identity_id: ownerStableIdentityId,
        role_label: "owner",
        message:
          `Share room_id '${room_id}' and its token with your peers. The token is ` +
          `required to join — save it now, it is never shown in any public listing.`,
      }, { headers: corsHeaders });
    }

    const capabilityMatch = path.match(
      /^\/capabilities\/([A-Z0-9]{6})\/(agents|skills)(?:\/([^/]+))?$/i,
    );
    if (capabilityMatch && request.method === "GET") {
      const room_id = capabilityMatch[1].toUpperCase();
      const collection = capabilityMatch[2].toLowerCase();
      const entryId = capabilityMatch[3]
        ? decodeURIComponent(capabilityMatch[3])
        : undefined;
      const token = readRoomToken(request, url);

      const registry = env.AGENTLINK_REGISTRY.get(
        env.AGENTLINK_REGISTRY.idFromName("global"),
      );
      const checkResp = await registry.fetch(new Request(
        `http://internal/check?room_id=${room_id}&token=${encodeURIComponent(token)}`,
      ));
      const check = await checkResp.json() as { ok: boolean; error?: string };

      if (!check.ok) {
        return Response.json(
          { success: false, error: check.error || "Unauthorized" },
          { status: 403, headers: corsHeaders },
        );
      }

      const roomDO = env.AGENTLINK_ROOM.get(
        env.AGENTLINK_ROOM.idFromName(room_id),
      );
      const internalPath = entryId
        ? `http://internal/capabilities/${collection}/${encodeURIComponent(entryId)}?room_id=${room_id}`
        : `http://internal/capabilities/${collection}?room_id=${room_id}`;
      const roomResp = await roomDO.fetch(new Request(internalPath));
      return new Response(await roomResp.text(), {
        status: roomResp.status,
        headers: {
          ...corsHeaders,
          "Content-Type": roomResp.headers.get("Content-Type") ?? "application/json",
        },
      });
    }

    const taskMatch = path.match(/^\/tasks\/([A-Z0-9]{6})(?:\/([^/]+))?$/i);
    if (taskMatch && request.method === "GET") {
      const room_id = taskMatch[1].toUpperCase();
      const taskId = taskMatch[2]
        ? decodeURIComponent(taskMatch[2])
        : undefined;
      const token = readRoomToken(request, url);

      const registry = env.AGENTLINK_REGISTRY.get(
        env.AGENTLINK_REGISTRY.idFromName("global"),
      );
      const checkResp = await registry.fetch(new Request(
        `http://internal/check?room_id=${room_id}&token=${encodeURIComponent(token)}`,
      ));
      const check = await checkResp.json() as { ok: boolean; error?: string };

      if (!check.ok) {
        return Response.json(
          { success: false, error: check.error || "Unauthorized" },
          { status: 403, headers: corsHeaders },
        );
      }

      const roomDO = env.AGENTLINK_ROOM.get(
        env.AGENTLINK_ROOM.idFromName(room_id),
      );
      const internalPath = taskId
        ? `http://internal/tasks/${encodeURIComponent(taskId)}?room_id=${room_id}`
        : `http://internal/tasks?room_id=${room_id}`;
      const roomResp = await roomDO.fetch(new Request(internalPath));
      return new Response(await roomResp.text(), {
        status: roomResp.status,
        headers: {
          ...corsHeaders,
          "Content-Type": roomResp.headers.get("Content-Type") ?? "application/json",
        },
      });
    }

    // ── WS /connect/:room_id ── join room via WebSocket
    const wsMatch = path.match(/^\/connect\/([A-Z0-9]{6})$/i);
    if (wsMatch) {
      const room_id = wsMatch[1].toUpperCase();
      const agentName = url.searchParams.get("name") || `agent-${generateId(4)}`;
      const token = url.searchParams.get("token") || "";
      const stableAgentIdentityId =
        sanitizeStableAgentIdentityId(url.searchParams.get("stable_agent_identity_id"));

      // Verifikasi room ada + token valid (via registry)
      const registry = env.AGENTLINK_REGISTRY.get(
        env.AGENTLINK_REGISTRY.idFromName("global")
      );
      const checkResp = await registry.fetch(new Request(
        `http://internal/check?room_id=${room_id}&token=${encodeURIComponent(token)}`
      ));
      const check = await checkResp.json() as { ok: boolean; error?: string };

      if (!check.ok) {
        return new Response(check.error || "Unauthorized", { status: 403 });
      }

      // Forward ke Durable Object room
      const roomDO = env.AGENTLINK_ROOM.get(
        env.AGENTLINK_ROOM.idFromName(room_id)
      );

      // Tambahkan header agent name untuk DO
      const newReq = new Request(request.url, {
        method: request.method,
        headers: {
          ...Object.fromEntries(request.headers),
          "X-Agent-Name": agentName,
          "X-Room-Id": room_id,
          ...(stableAgentIdentityId
            ? { "X-Stable-Agent-Identity-Id": stableAgentIdentityId }
            : {}),
        },
      });

      return roomDO.fetch(newReq);
    }

    if (request.method === "GET") {
      const assetResponse = await env.ASSETS.fetch(request);
      if (assetResponse.status !== 404) return assetResponse;
    }

    return new Response("Not Found", { status: 404, headers: corsHeaders });
  },
};

function acceptsMarkdown(request: Request): boolean {
  return request.headers.get("Accept")?.split(",").some((value) =>
    value.trim().split(";")[0] === "text/markdown"
  ) ?? false;
}

function markdownResponse(body: string): Response {
  return new Response(body, {
    headers: {
      "Content-Type": "text/markdown; charset=utf-8",
      "Vary": "Accept",
      "x-markdown-tokens": String(Math.ceil(body.length / 4)),
    },
  });
}

function withDiscoveryLinks(response: Response): Response {
  const headers = new Headers(response.headers);
  headers.append("Link", '</.well-known/api-catalog>; rel="api-catalog"');
  headers.append("Link", '</.well-known/ai-catalog.json>; rel="describedby"; type="application/json"');
  headers.append("Link", '</info>; rel="service-doc"; type="application/json"');
  headers.append("Vary", "Accept");
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
}

function buildSitemap(origin: string): string {
  const pages = ["/", "/info", "/health", "/auth.md"];
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...pages.map((page) => `  <url><loc>${origin}${page}</loc></url>`),
    "</urlset>",
    "",
  ].join("\n");
}

function buildDiscoveryResponse(path: string, origin: string): Response | null {
  const documents: Record<string, { body: unknown; contentType?: string }> = {
    "/.well-known/api-catalog": {
      body: {
        linkset: [{
          anchor: `${origin}/info`,
          "service-desc": [{ href: `${origin}/openapi.json`, type: "application/vnd.oai.openapi+json;version=3.1" }],
          "service-doc": [{ href: `${origin}/info`, type: "application/json" }],
          status: [{ href: `${origin}/health`, type: "application/json" }],
        }],
      },
      contentType: 'application/linkset+json; profile="https://www.rfc-editor.org/info/rfc9727"',
    },
    "/.well-known/oauth-protected-resource": {
      body: {
        resource: origin,
        resource_documentation: `${origin}/auth.md`,
      },
    },
    "/.well-known/mcp/server-card.json": {
      body: {
        serverInfo: { name: "AgentLink", version: "3.0.1" },
        transport: { type: "websocket", endpoint: `${origin}/connect/{room_id}` },
        capabilities: { tools: true, resources: true, prompts: false },
        documentation: `${origin}/info`,
      },
    },
    "/.well-known/ai-catalog.json": {
      body: {
        specVersion: "1.0",
        host: { displayName: "AgentLink", identifier: `https://${new URL(origin).host}` },
        entries: [
          {
            identifier: `urn:air:${new URL(origin).host}:api:agentlink`,
            displayName: "AgentLink REST and WebSocket API",
            type: "application/json",
            url: `${origin}/info`,
            representativeQueries: ["list AgentLink API endpoints", "create a private agent room"],
          },
          {
            identifier: `urn:air:${new URL(origin).host}:mcp:ssyubix`,
            displayName: "ssyubix local MCP server",
            type: "application/mcp-server",
            url: "https://pypi.org/project/ssyubix/",
            representativeQueries: ["connect two agents through AgentLink", "send a message between agents"],
          },
        ],
      },
    },
    "/.well-known/agent-skills/index.json": {
      body: {
        $schema: "https://schemas.agentskills.io/discovery/0.2.0/schema.json",
        skills: [{
          name: "agentlink-room-operations",
          type: "skill-md",
          description: "Coordinate trusted AI agents in private AgentLink rooms using the REST API and WebSocket relay.",
          url: `${origin}/agent-skills/agentlink-room-operations/SKILL.md`,
          digest: "sha256:e9de7cbc2ca482d79126ba54676d886e2efe2dc087552eb75e7ce6040302c7f6",
        }],
      },
    },
  };
  const document = documents[path];
  if (!document) return null;
  return new Response(JSON.stringify(document.body, null, 2), {
    headers: {
      "Content-Type": document.contentType ?? "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "public, max-age=3600",
    },
  });
}

function buildHomepageMarkdown(origin: string): string {
  return `# AgentLink\n\nCross-device MCP relay for private AI-agent rooms.\n\n- Server information: ${origin}/info\n- API catalog: ${origin}/.well-known/api-catalog\n- API specification: ${origin}/openapi.json\n`;
}

function buildAuthMarkdown(origin: string): string {
  return `# AgentLink auth.md

## Audience

AgentLink is for trusted AI agents that coordinate work across devices in a private room. It does not provide user accounts, OAuth, OIDC, or dynamic client registration.

## Agent registration

Agent registration is supported through private room provisioning. There is no browser signup flow: an agent registers by being provisioned into a room by the room owner.

### Registration method: room provisioning

The agent registration and provisioning endpoint is \`${origin}/rooms\`. Use \`POST ${origin}/rooms\` to create a room and receive the credentials that are used to register trusted peer agents:

Room provisioning is agent-driven and does not create a user account. Create a room with:

\`\`\`http
POST ${origin}/rooms
Content-Type: application/json

{
  "name": "research",
  "owner_stable_identity_id": "agent-owner-1"
}
\`\`\`

The response contains a private \`room_id\` and one-time \`token\`. This room ID and token are the registration credentials for trusted agents. Share both only with trusted agents. The full endpoint contract is available at ${origin}/info and ${origin}/openapi.json.

An agent completes registration by connecting with the provisioned room ID and token, then identifying itself with \`name\` and, optionally, \`stable_agent_identity_id\`. REST registration checks use the \`X-Room-Token\` header; WebSocket registration uses the \`token\` query parameter.

## Supported authentication methods

- REST room reads use the \`X-Room-Token\` header.
- WebSocket connections use the room token in the required \`token\` query parameter for \`WS ${origin}/connect/{room_id}\`.
- \`GET ${origin}/rooms\` exposes aggregate activity only and does not expose room identifiers, names, or tokens.

These are private room credentials, not OAuth bearer access tokens. AgentLink does not publish an OAuth authorization server, token endpoint, JWKS endpoint, claim URI, or revocation endpoint, and does not support OAuth dynamic client registration.

## Credential handling

Keep the room ID and token confidential. Do not place REST tokens in URLs, logs, public metadata, or prompts. A room creator is responsible for provisioning access to peers; agents must not guess room credentials or disclose them to untrusted parties.
`;
}

// ─── Durable Object: Room ─────────────────────────────────────────────────────

export class AgentLinkRoom extends DurableObject<Env> {
  private sequenceCounter: number | null = null;
  private lastHydratedAt: string | null = null;
  private roomIdCache: string | null = null;
  /** Jumlah agent yang terakhir dikirim ke registry; null artinya belum pernah. */
  private lastReportedAgentCount: number | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname.startsWith("/capabilities/")) {
      const now = new Date().toISOString();
      await this.ensureActiveSessionsHydrated(now);
      return this.handleCapabilityRequest(url);
    }
    if (request.method === "GET" && url.pathname.startsWith("/tasks")) {
      const now = new Date().toISOString();
      await this.ensureActiveSessionsHydrated(now);
      return this.handleTaskRequest(url);
    }

    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const agentName = request.headers.get("X-Agent-Name") || "unknown";
    const roomId    = request.headers.get("X-Room-Id") || "unknown";
    const stableAgentIdentityId =
      sanitizeStableAgentIdentityId(request.headers.get("X-Stable-Agent-Identity-Id")) ||
      sanitizeStableAgentIdentityId(new URL(request.url).searchParams.get("stable_agent_identity_id"));
    const sessionId = new URL(request.url).searchParams.get("session_id") || generateId(16);
    const now = new Date().toISOString();
    await this.ensureActiveSessionsHydrated(now);
    const session = await this.resolveSession({
      sessionId,
      agentName,
      now,
    });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Pakai Hibernation API
    this.ctx.acceptWebSocket(server, [
      session.agentId,
      agentName,
      roomId,
      sessionId,
      stableAgentIdentityId || "",
    ]);
    let state: AgentSessionState = {
      session_id: sessionId,
      agent_id: session.agentId,
      stable_agent_identity_id: stableAgentIdentityId,
      name: agentName,
      room_id: roomId,
      joined_at: session.joinedAt,
      last_seen_at: now,
      presence: "online",
    };
    this.writeAgentState(server, state);
    state = await this.maybeCheckpointSessionState(server, {
      previousState: state,
      nextState: state,
      force: true,
    });
    this.closeDuplicateSessions(server, sessionId);
    await this.upsertCapabilityState(state);
    await this.rememberRoomId(roomId);
    // Jangan lapor di sini: biarkan alarm yang melakukannya supaya join tetap cepat.
    await this.scheduleTransientCheckpoint(now);

    const joinSequence = await this.nextSequence();
    const heartbeat = buildHeartbeatConfig();
    const roomMeta = await this.loadRoomMeta(roomId);
    const roomRoles = normalizeRoomRoleState(roomMeta ?? undefined);
    const selfRoleLabel = resolveRoomRoleLabel(
      roomRoles,
      state.stable_agent_identity_id,
    );

    // Kirim info ke agent yang baru join
    const existingAgents = this.listActiveAgentsWithRoles(roomRoles, {
      excludeAgentId: state.agent_id,
      excludeSessionId: sessionId,
    });

    server.send(JSON.stringify({
      type: "welcome",
      agent_id: state.agent_id,
      stable_agent_identity_id: state.stable_agent_identity_id,
      name: agentName,
      room_id: roomId,
      room_name: roomMeta?.name ?? roomId,
      is_private: true,
      room_role: selfRoleLabel,
      owner_stable_identity_id: roomRoles.owner_stable_identity_id,
      admin_stable_identity_ids: roomRoles.admin_stable_identity_ids,
      last_sequence: joinSequence,
      joined_at: state.joined_at,
      last_seen_at: state.last_seen_at,
      presence: state.presence,
      role_label: selfRoleLabel,
      session_resumed: session.reconnected,
      heartbeat_interval_seconds: heartbeat.heartbeat_interval_seconds,
      heartbeat_timeout_seconds: heartbeat.heartbeat_timeout_seconds,
      reconnect_window_seconds: heartbeat.reconnect_window_seconds,
      presence_checkpoint_interval_seconds: heartbeat.presence_checkpoint_interval_seconds,
      agents: existingAgents,
      message: session.reconnected
        ? `Reconnected to room '${roomId}'.`
        : `Welcome to room '${roomId}'.`,
    }));

    // Broadcast ke semua agent lain: ada yang join
    const eventName = session.reconnected ? "agent_reconnected" : "agent_joined";
    this.broadcast(server, JSON.stringify(createRoomEvent({
      roomId,
      sequence: joinSequence,
      timestamp: now,
      event: eventName,
      agentId: state.agent_id,
      stableAgentIdentityId: state.stable_agent_identity_id,
      name: agentName,
      roleLabel: selfRoleLabel,
      presence: state.presence,
      joinedAt: state.joined_at,
      lastSeenAt: state.last_seen_at,
      sessionResumed: session.reconnected,
    })));

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, raw: string | ArrayBuffer): Promise<void> {
    let msg: WsMessage;
    try {
      msg = JSON.parse(typeof raw === "string" ? raw : new TextDecoder().decode(raw));
    } catch {
      ws.send(JSON.stringify({ type: "error", error: "Invalid JSON" }));
      return;
    }

    await this.ensureActiveSessionsHydrated(new Date().toISOString());
    const agentState = await this.touchAgentState(ws);
    const agentId = agentState.agent_id;
    const name = agentState.name;
    const roomId = agentState.room_id;

    // Handle ping
    if (msg.type === "ping") {
      const heartbeat = buildHeartbeatConfig();
      ws.send(JSON.stringify({
        type: "pong",
        room_id: roomId,
        agent_id: agentId,
        stable_agent_identity_id: agentState.stable_agent_identity_id,
        presence: agentState.presence,
        timestamp: agentState.last_seen_at,
        last_seen_at: agentState.last_seen_at,
        heartbeat_interval_seconds: heartbeat.heartbeat_interval_seconds,
        heartbeat_timeout_seconds: heartbeat.heartbeat_timeout_seconds,
        presence_checkpoint_interval_seconds: heartbeat.presence_checkpoint_interval_seconds,
        echo_sent_at: typeof msg.sent_at === "string" ? msg.sent_at : undefined,
      }));
      return;
    }

    // Handle send ke target tertentu (direct message)
    if (msg.type === "send" && msg.to) {
      const targetId = msg.to as string;
      const sockets  = this.ctx.getWebSockets();
      let delivered  = false;
      let messageId: string | undefined;
      let sequence: number | undefined;
      const requestId = typeof msg.request_id === "string" ? msg.request_id : undefined;
      const timestamp = new Date().toISOString();

      for (const sock of sockets) {
        const sockTags = this.ctx.getTags(sock);
        if (sockTags[0] === targetId) {
          sequence = await this.nextSequence();
          const payload = createRoomMessage({
            roomId,
            sequence,
            timestamp,
            from: agentId,
            fromName: name,
            content: msg.content,
            msgType: typeof msg.msg_type === "string" ? msg.msg_type : "text",
          });
          messageId = payload.message_id;
          sock.send(JSON.stringify(payload));
          delivered = true;
          break;
        }
      }

      ws.send(JSON.stringify(createAck({
        action: "send",
        roomId,
        requestId,
        delivered,
        recipientCount: delivered ? 1 : 0,
        timestamp,
        messageId,
        sequence,
        to: targetId,
      })));
      return;
    }

    // Handle broadcast ke semua
    if (msg.type === "broadcast") {
      const timestamp = new Date().toISOString();
      const sequence = await this.nextSequence();
      const payload = createRoomMessage({
        roomId,
        sequence,
        timestamp,
        from: agentId,
        fromName: name,
        content: msg.content,
        msgType: typeof msg.msg_type === "string" ? msg.msg_type : "text",
        broadcast: true,
      });
      const recipientCount = this.broadcast(ws, JSON.stringify(payload));
      const requestId = typeof msg.request_id === "string" ? msg.request_id : undefined;

      ws.send(JSON.stringify(createAck({
        action: "broadcast",
        roomId,
        requestId,
        delivered: recipientCount > 0,
        recipientCount,
        timestamp,
        messageId: payload.message_id,
        sequence: payload.sequence,
        broadcast: true,
      })));
      return;
    }

    if (msg.type === "task_offer") {
      const timestamp = new Date().toISOString();
      const requestId = typeof msg.request_id === "string" ? msg.request_id : undefined;
      const title = typeof msg.title === "string" ? msg.title.trim() : "";
      if (!title || title.length > 140) {
        ws.send(JSON.stringify({
          type: "error",
          error: "A task title is required and must be at most 140 characters.",
          request_id: requestId,
          code: "invalid_task_title",
        }));
        return;
      }
      const targetAgentId = typeof msg.to_agent_id === "string" ? msg.to_agent_id : "";
      if (!targetAgentId) {
        ws.send(JSON.stringify({
          type: "error",
          error: "to_agent_id is required for a delegation offer.",
          request_id: requestId,
          code: "missing_task_target",
        }));
        return;
      }
      const targetState = this.findActiveAgentById(targetAgentId);
      if (!targetState) {
        ws.send(JSON.stringify({
          type: "error",
          error: `Target agent '${targetAgentId}' is not active in this room.`,
          request_id: requestId,
          code: "task_target_not_active",
        }));
        return;
      }
      if (!targetState.stable_agent_identity_id) {
        ws.send(JSON.stringify({
          type: "error",
          error: `Target agent '${targetAgentId}' has no stable identity yet.`,
          request_id: requestId,
          code: "task_target_missing_identity",
        }));
        return;
      }

      const pointOfContactAgentId =
        typeof msg.point_of_contact_agent_id === "string" && msg.point_of_contact_agent_id
          ? msg.point_of_contact_agent_id
          : agentId;
      const pointOfContactState =
        pointOfContactAgentId === agentId
          ? agentState
          : this.findActiveAgentById(pointOfContactAgentId);
      if (!pointOfContactState) {
        ws.send(JSON.stringify({
          type: "error",
          error: `point_of_contact_agent_id '${pointOfContactAgentId}' is not active in this room.`,
          request_id: requestId,
          code: "task_invalid_point_of_contact",
        }));
        return;
      }

      const priority: TaskPriority =
        msg.priority === "low" || msg.priority === "high" || msg.priority === "normal"
          ? msg.priority
          : "normal";
      const taskId =
        typeof msg.task_id === "string" && msg.task_id.trim()
          ? msg.task_id.trim()
          : `TASK_${generateId(10)}`;
      const manifest = await this.loadTaskRegistryManifest(timestamp);
      const { changed, task } = createDelegationOffer(manifest, {
        taskId,
        title,
        delegatedBy: agentId,
        delegatedByIdentityId: agentState.stable_agent_identity_id,
        offeredToAgentId: targetState.agent_id,
        offeredToIdentityId: targetState.stable_agent_identity_id,
        pointOfContactAgentId: pointOfContactState.agent_id,
        pointOfContactIdentityId: pointOfContactState.stable_agent_identity_id,
        createdAt: timestamp,
        updatedAt: timestamp,
        priority,
      });
      if (changed) {
        await this.ctx.storage.put(ROOM_TASK_REGISTRY_KEY, manifest);
      }
      const sequence = changed ? await this.nextSequence() : undefined;
      if (sequence !== undefined) {
        this.broadcast(ws, JSON.stringify(createRoomEvent({
          roomId,
          sequence,
          timestamp,
          event: "task_offered",
          agentId,
          stableAgentIdentityId: agentState.stable_agent_identity_id,
          name,
          taskId: task.task_id,
          task: this.toTaskEventPayload(task),
        })));
      }
      ws.send(JSON.stringify(createAck({
        action: "task_offer",
        roomId,
        requestId,
        delivered: true,
        recipientCount: sequence ? Math.max(0, this.ctx.getWebSockets().length - 1) : 0,
        timestamp,
        messageId: sequence ? `${roomId}:${sequence}` : undefined,
        sequence,
        taskId: task.task_id,
      })));
      return;
    }

    if (msg.type === "task_accept" || msg.type === "task_reject" || msg.type === "task_defer") {
      const timestamp = new Date().toISOString();
      const requestId = typeof msg.request_id === "string" ? msg.request_id : undefined;
      const taskId = typeof msg.task_id === "string" ? msg.task_id.trim() : "";
      if (!taskId) {
        ws.send(JSON.stringify({
          type: "error",
          error: "task_id is required.",
          request_id: requestId,
          code: "missing_task_id",
        }));
        return;
      }

      const manifest = await this.loadTaskRegistryManifest(timestamp);
      const reason =
        typeof msg.reason === "string" && msg.reason.trim()
          ? msg.reason.trim().slice(0, 240)
          : undefined;
      if (
        msg.type === "task_defer"
        && msg.deferred_until !== undefined
        && (
          typeof msg.deferred_until !== "string"
          || Number.isNaN(Date.parse(msg.deferred_until))
        )
      ) {
        ws.send(JSON.stringify({
          type: "error",
          error: "deferred_until must be a valid ISO-8601 timestamp.",
          request_id: requestId,
          code: "invalid_task_deferred_until",
        }));
        return;
      }
      const deferUntil =
        typeof msg.deferred_until === "string" && !Number.isNaN(Date.parse(msg.deferred_until))
          ? msg.deferred_until
          : null;

      const result =
        msg.type === "task_accept"
          ? acceptDelegationOffer(manifest, {
            taskId,
            actorAgentId: agentId,
            actorIdentityId: agentState.stable_agent_identity_id,
            updatedAt: timestamp,
            leaseUntil: new Date(Date.parse(timestamp) + 60 * 60 * 1000).toISOString(),
          })
          : msg.type === "task_reject"
            ? rejectDelegationOffer(manifest, {
              taskId,
              actorAgentId: agentId,
              actorIdentityId: agentState.stable_agent_identity_id,
              updatedAt: timestamp,
              reason,
            })
            : deferDelegationOffer(manifest, {
              taskId,
              actorAgentId: agentId,
              actorIdentityId: agentState.stable_agent_identity_id,
              updatedAt: timestamp,
              deferredUntil: deferUntil,
              reason,
            });

      if (result.error || !result.task) {
        ws.send(JSON.stringify({
          type: "error",
          error: result.error || "Could not update the delegation task.",
          request_id: requestId,
          code: `task_${msg.type}_failed`,
        }));
        return;
      }

      if (result.changed) {
        await this.ctx.storage.put(ROOM_TASK_REGISTRY_KEY, manifest);
      }
      const eventName =
        msg.type === "task_accept"
          ? "task_accepted"
          : msg.type === "task_reject"
            ? "task_rejected"
            : "task_deferred";
      const sequence = result.changed ? await this.nextSequence() : undefined;
      if (sequence !== undefined) {
        this.broadcast(ws, JSON.stringify(createRoomEvent({
          roomId,
          sequence,
          timestamp,
          event: eventName,
          agentId,
          stableAgentIdentityId: agentState.stable_agent_identity_id,
          name,
          taskId: result.task.task_id,
          task: this.toTaskEventPayload(result.task),
        })));
      }
      ws.send(JSON.stringify(createAck({
        action: msg.type,
        roomId,
        requestId,
        delivered: true,
        recipientCount: sequence ? Math.max(0, this.ctx.getWebSockets().length - 1) : 0,
        timestamp,
        messageId: sequence ? `${roomId}:${sequence}` : undefined,
        sequence,
        taskId: result.task.task_id,
      })));
      return;
    }

    if (msg.type === "room_admin_add" || msg.type === "room_admin_remove") {
      const timestamp = new Date().toISOString();
      const requestId = typeof msg.request_id === "string" ? msg.request_id : undefined;
      const targetAgentId = typeof msg.target_agent_id === "string"
        ? msg.target_agent_id.trim()
        : "";
      if (!targetAgentId) {
        ws.send(JSON.stringify({
          type: "error",
          error: "target_agent_id is required.",
          request_id: requestId,
          code: "missing_room_role_target",
        }));
        return;
      }
      const targetState = this.findActiveAgentById(targetAgentId);
      if (!targetState) {
        ws.send(JSON.stringify({
          type: "error",
          error: `Target agent '${targetAgentId}' is not active in this room.`,
          request_id: requestId,
          code: "room_role_target_not_active",
        }));
        return;
      }
      if (!targetState.stable_agent_identity_id) {
        ws.send(JSON.stringify({
          type: "error",
          error: `Target agent '${targetAgentId}' has no stable identity yet.`,
          request_id: requestId,
          code: "room_role_target_missing_identity",
        }));
        return;
      }

      const roleMutation = await this.mutateRoomAdminRole({
        roomId,
        actorStableIdentityId: agentState.stable_agent_identity_id,
        targetStableIdentityId: targetState.stable_agent_identity_id,
        action: msg.type === "room_admin_add" ? "grant" : "revoke",
      });

      if (!roleMutation.ok) {
        ws.send(JSON.stringify({
          type: "error",
          error: roleMutation.error || "Could not update the room role.",
          request_id: requestId,
          code: msg.type === "room_admin_add"
            ? "room_admin_add_failed"
            : "room_admin_remove_failed",
        }));
        return;
      }

      const roleAck = this.buildRoomRoleAck(
        roleMutation.roleState,
        agentState.stable_agent_identity_id,
        targetState,
      );
      const sequence = roleMutation.changed ? await this.nextSequence() : undefined;
      if (sequence !== undefined) {
        this.broadcast(ws, JSON.stringify(createRoomEvent({
          roomId,
          sequence,
          timestamp,
          event: "room_roles_updated",
          agentId: agentId,
          stableAgentIdentityId: agentState.stable_agent_identity_id,
          name,
          roleLabel: roleAck.role_label,
          roomRoles: roleMutation.roleState,
          targetAgentId: targetState.agent_id,
          targetStableIdentityId: targetState.stable_agent_identity_id,
          targetRoleLabel: roleAck.target_role_label,
        })));
      }
      ws.send(JSON.stringify(createAck({
        action: msg.type,
        roomId,
        requestId,
        delivered: true,
        recipientCount: sequence ? Math.max(0, this.ctx.getWebSockets().length - 1) : 0,
        timestamp,
        sequence,
        messageId: sequence ? `${roomId}:${sequence}` : undefined,
        roleLabel: roleAck.role_label,
        roomRoles: roleMutation.roleState,
        targetAgentId: roleAck.target_agent_id,
        targetStableIdentityId: roleAck.target_stable_identity_id,
        targetRoleLabel: roleAck.target_role_label,
      })));
      return;
    }

    if (msg.type === "capability_upsert") {
      const timestamp = new Date().toISOString();
      const requestId = typeof msg.request_id === "string" ? msg.request_id : undefined;
      const validation = validateCapabilityProfilePatch(pickProvided(msg, [
        "summary",
        "version",
        "tool_access",
        "constraints",
        "max_concurrent_tasks",
        "current_load",
        "skills",
      ]));
      if (!validation.ok || !validation.patch) {
        ws.send(JSON.stringify({
          type: "error",
          error: validation.errors.join(" "),
          request_id: requestId,
          code: "invalid_capability_profile",
          allowed_availability: [...CAPABILITY_AVAILABILITY_VALUES],
        }));
        return;
      }

      const { changed } = await this.applyCapabilityMutation(agentState, {
        patch: validation.patch,
        timestamp,
      });
      const sequence = await this.broadcastCapabilityChange(ws, {
        agentState,
        timestamp,
        event: "capability_updated",
        changed,
      });

      ws.send(JSON.stringify(createAck({
        action: "capability_upsert",
        roomId,
        requestId,
        delivered: true,
        recipientCount: sequence ? Math.max(0, this.ctx.getWebSockets().length - 1) : 0,
        timestamp,
        sequence: sequence ?? undefined,
        messageId: sequence ? `${roomId}:${sequence}` : undefined,
      })));
      return;
    }

    if (msg.type === "capability_set_availability") {
      const timestamp = new Date().toISOString();
      const requestId = typeof msg.request_id === "string" ? msg.request_id : undefined;
      const validation = validateCapabilityProfilePatch(
        pickProvided(msg, ["availability", "current_load"]),
        { allowAvailability: true, availabilityOnly: true },
      );
      if (!validation.ok || !validation.patch) {
        ws.send(JSON.stringify({
          type: "error",
          error: validation.errors.join(" "),
          request_id: requestId,
          code: "invalid_capability_availability",
          allowed_availability: [...CAPABILITY_AVAILABILITY_VALUES],
        }));
        return;
      }

      const { changed } = await this.applyCapabilityMutation(agentState, {
        patch: validation.patch,
        timestamp,
      });
      const sequence = await this.broadcastCapabilityChange(ws, {
        agentState,
        timestamp,
        event: "capability_updated",
        changed,
      });

      ws.send(JSON.stringify(createAck({
        action: "capability_set_availability",
        roomId,
        requestId,
        delivered: true,
        recipientCount: sequence ? Math.max(0, this.ctx.getWebSockets().length - 1) : 0,
        timestamp,
        sequence: sequence ?? undefined,
        messageId: sequence ? `${roomId}:${sequence}` : undefined,
      })));
      return;
    }

    if (msg.type === "capability_remove") {
      const timestamp = new Date().toISOString();
      const requestId = typeof msg.request_id === "string" ? msg.request_id : undefined;
      const removed = await this.removeStoredCapabilityProfile(agentState.agent_id, timestamp);
      const sequence = await this.broadcastCapabilityChange(ws, {
        agentState,
        timestamp,
        event: "capability_removed",
        changed: removed,
      });

      ws.send(JSON.stringify(createAck({
        action: "capability_remove",
        roomId,
        requestId,
        delivered: true,
        recipientCount: sequence ? Math.max(0, this.ctx.getWebSockets().length - 1) : 0,
        timestamp,
        sequence: sequence ?? undefined,
        messageId: sequence ? `${roomId}:${sequence}` : undefined,
      })));
      return;
    }

    ws.send(JSON.stringify({
      type: "error",
      error: `Unknown type: ${msg.type}`,
    }));
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string): Promise<void> {
    const state = this.readAgentState(ws);
    if (state.session_id && this.hasActiveSession(state.session_id, ws)) {
      return;
    }

    const timestamp = new Date().toISOString();
    const offlineState: AgentSessionState = {
      ...state,
      last_seen_at: timestamp,
      presence: "offline",
    };
    await this.maybeCheckpointSessionState(ws, {
      previousState: state,
      nextState: offlineState,
      force: true,
    });
    await this.upsertCapabilityState(offlineState);
    // Agent keluar mengubah jumlah; alarm yang akan melaporkannya (ter-debounce).
    await this.scheduleTransientCheckpoint(timestamp);
    const sequence = await this.nextSequence();

    // Broadcast ke semua: ada yang leave
    this.broadcast(ws, JSON.stringify(createRoomEvent({
      roomId: state.room_id,
      sequence,
      timestamp,
      event: "agent_left",
      agentId: state.agent_id,
      stableAgentIdentityId: state.stable_agent_identity_id,
      name: state.name,
      presence: offlineState.presence,
      joinedAt: state.joined_at,
      lastSeenAt: offlineState.last_seen_at,
    })));
  }

  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    console.error("WebSocket error:", error);
    ws.close(1011, "Internal error");
  }

  async alarm(): Promise<void> {
    const now = new Date().toISOString();
    await this.flushTransientSessionCheckpoints(now);
    await this.reportAgentCountIfChanged();
  }

  private async getCurrentSequence(): Promise<number> {
    if (this.sequenceCounter === null) {
      this.sequenceCounter = (await this.ctx.storage.get<number>("room:sequence")) ?? 0;
    }
    return this.sequenceCounter;
  }

  private async nextSequence(): Promise<number> {
    const next = (await this.getCurrentSequence()) + 1;
    this.sequenceCounter = next;
    await this.ctx.storage.put("room:sequence", next);
    return next;
  }

  private readAgentState(ws: WebSocket): AgentSessionState {
    const attachment = ws.deserializeAttachment();
    if (attachment && typeof attachment === "object") {
      return attachment as AgentSessionState;
    }

    const tags = this.ctx.getTags(ws);
    const timestamp = new Date().toISOString();
    return {
      session_id: tags[3] || "",
      agent_id: tags[0] || "unknown",
      stable_agent_identity_id: tags[4] || undefined,
      name: tags[1] || "unknown",
      room_id: tags[2] || "unknown",
      joined_at: timestamp,
      last_seen_at: timestamp,
      presence: "online",
      checkpointed_at: undefined,
    };
  }

  private writeAgentState(ws: WebSocket, state: AgentSessionState): AgentSessionState {
    ws.serializeAttachment(state);
    return state;
  }

  private async touchAgentState(ws: WebSocket): Promise<AgentSessionState> {
    const previousState = this.readAgentState(ws);
    const nextState = toHydratedPresenceState(previousState, new Date().toISOString());
    this.writeAgentState(ws, nextState);
    return this.maybeCheckpointSessionState(ws, {
      previousState,
      nextState,
    });
  }

  private getRegistryStub() {
    return this.env.AGENTLINK_REGISTRY.get(
      this.env.AGENTLINK_REGISTRY.idFromName("global"),
    );
  }

  /** Simpan ID room sekali saja, supaya alarm bisa melapor tanpa WebSocket aktif. */
  private async rememberRoomId(roomId: string): Promise<void> {
    if (this.roomIdCache === roomId) {
      return;
    }
    this.roomIdCache = roomId;
    const stored = await this.ctx.storage.get<string>(ROOM_ID_KEY);
    if (stored !== roomId) {
      await this.ctx.storage.put(ROOM_ID_KEY, roomId);
    }
  }

  private async resolveRoomId(): Promise<string | null> {
    if (this.roomIdCache) {
      return this.roomIdCache;
    }
    const stored = await this.ctx.storage.get<string>(ROOM_ID_KEY);
    this.roomIdCache = stored ?? null;
    return this.roomIdCache;
  }

  /**
   * Laporkan jumlah agent aktif ke registry, tapi hanya bila berubah sejak laporan
   * terakhir — room yang alarmnya menyala untuk urusan lain tidak menulis apa-apa.
   *
   * Dipanggil dari alarm, bukan dari jalur join/leave, supaya debounce 5 detik yang
   * sudah ada yang membatasi lajunya dan join tidak ikut melambat.
   */
  private async reportAgentCountIfChanged(): Promise<void> {
    const roomId = await this.resolveRoomId();
    if (!roomId) {
      return;
    }

    const count = this.listActiveAgents().length;
    if (this.lastReportedAgentCount === count) {
      return;
    }

    try {
      await this.getRegistryStub().fetch(new Request(
        "http://internal/set-agent-count",
        {
          method: "POST",
          body: JSON.stringify({ room_id: roomId, agent_count: count }),
        },
      ));
      this.lastReportedAgentCount = count;
    } catch (error) {
      // Angka dashboard tidak boleh menghambat room. Biarkan lastReported apa adanya
      // supaya laporan berikutnya mencoba lagi.
      console.error("Failed to report agent_count to the registry:", error);
    }
  }

  private async loadRoomMeta(roomId: string): Promise<RoomMeta | null> {
    const response = await this.getRegistryStub().fetch(
      new Request(`http://internal/room?room_id=${encodeURIComponent(roomId)}`),
    );
    const payload = await response.json() as {
      ok?: boolean;
      room?: RoomMeta;
      error?: string;
    };
    if (!response.ok || !payload.ok || !payload.room) {
      return null;
    }
    return payload.room;
  }

  private async loadRoomRoleState(roomId: string): Promise<StoredRoomRoleState> {
    const room = await this.loadRoomMeta(roomId);
    return normalizeRoomRoleState(room ?? undefined);
  }

  private listActiveAgentsWithRoles(
    roleState: StoredRoomRoleState,
    options: {
      excludeAgentId?: string;
      excludeSessionId?: string;
    } = {},
  ): AgentInfo[] {
    return this.listActiveAgents(options).map((snapshot) => ({
      ...snapshot,
      role_label: resolveRoomRoleLabel(
        roleState,
        snapshot.stable_agent_identity_id,
      ),
    }));
  }

  private buildRoomRoleAck(
    roleState: StoredRoomRoleState,
    actorStableIdentityId: string | undefined,
    targetState: AgentSessionState,
  ): RoomRoleAckPayload {
    return {
      owner_stable_identity_id: roleState.owner_stable_identity_id,
      admin_stable_identity_ids: roleState.admin_stable_identity_ids,
      role_label: resolveRoomRoleLabel(roleState, actorStableIdentityId),
      target_agent_id: targetState.agent_id,
      target_stable_identity_id: targetState.stable_agent_identity_id ?? "",
      target_role_label: resolveRoomRoleLabel(
        roleState,
        targetState.stable_agent_identity_id,
      ),
    };
  }

  private async mutateRoomAdminRole(params: {
    roomId: string;
    actorStableIdentityId?: string;
    targetStableIdentityId: string;
    action: "grant" | "revoke";
  }): Promise<{
    ok: boolean;
    changed: boolean;
    roleState: StoredRoomRoleState;
    error?: string;
  }> {
    const response = await this.getRegistryStub().fetch(new Request(
      `http://internal/${params.action === "grant" ? "grant-admin" : "revoke-admin"}`,
      {
        method: "POST",
        body: JSON.stringify({
          room_id: params.roomId,
          actor_stable_identity_id: params.actorStableIdentityId,
          target_stable_identity_id: params.targetStableIdentityId,
        }),
      },
    ));
    const payload = await response.json() as {
      ok?: boolean;
      changed?: boolean;
      error?: string;
      role_state?: StoredRoomRoleState;
    };
    return {
      ok: response.ok && payload.ok !== false,
      changed: payload.changed === true,
      roleState: normalizeRoomRoleState(payload.role_state),
      error: payload.error,
    };
  }

  private listActiveAgents(options: {
    excludeAgentId?: string;
    excludeSessionId?: string;
  } = {}): AgentPresenceSnapshot[] {
    const snapshots = new Map<string, AgentPresenceSnapshot>();
    for (const ws of this.ctx.getWebSockets()) {
      const state = this.readAgentState(ws);
      if (options.excludeAgentId && state.agent_id === options.excludeAgentId) {
        continue;
      }
      if (options.excludeSessionId && state.session_id === options.excludeSessionId) {
        continue;
      }
      snapshots.set(state.agent_id, toPresenceSnapshot(state));
    }
    return [...snapshots.values()];
  }

  private findActiveAgentById(agentId: string): AgentSessionState | null {
    for (const ws of this.ctx.getWebSockets()) {
      const state = this.readAgentState(ws);
      if (state.agent_id === agentId) {
        return state;
      }
    }
    return null;
  }

  private hasActiveSession(sessionId: string, excludedWs: WebSocket): boolean {
    return this.findActiveSession(sessionId, excludedWs) !== null;
  }

  private closeDuplicateSessions(currentWs: WebSocket, sessionId: string): void {
    if (!sessionId) {
      return;
    }

    for (const ws of this.ctx.getWebSockets()) {
      if (ws === currentWs) {
        continue;
      }
      if (this.readAgentState(ws).session_id === sessionId) {
        try {
          ws.close(1012, "Session resumed elsewhere");
        } catch {}
      }
    }
  }

  private async storeSessionState(state: AgentSessionState): Promise<void> {
    if (!state.session_id) {
      return;
    }

    const manifest = await this.loadSessionCheckpointManifest();
    manifest.sessions[state.session_id] = this.toStoredSession(state);
    this.pruneExpiredSessionCheckpoints(manifest, state.last_seen_at);
    manifest.updated_at = state.last_seen_at;
    await this.ctx.storage.put(ROOM_SESSION_CHECKPOINTS_KEY, manifest);
  }

  private async loadCapabilityRegistryManifest(
    now = new Date().toISOString(),
  ): Promise<CapabilityRegistryManifest> {
    const stored = await this.ctx.storage.get<CapabilityRegistryManifest>(
      ROOM_CAPABILITY_REGISTRY_KEY,
    );
    return createCapabilityRegistryManifest(stored, now);
  }

  private async loadTaskRegistryManifest(
    now = new Date().toISOString(),
  ): Promise<TaskRegistryManifest> {
    const stored = await this.ctx.storage.get<TaskRegistryManifest>(
      ROOM_TASK_REGISTRY_KEY,
    );
    return createTaskRegistryManifest(stored, now);
  }

  private toTaskEventPayload(task: StoredTaskManifest): TaskEventPayload {
    return {
      task_id: task.task_id,
      title: task.title,
      status: task.status,
      offer_state: task.offer_state,
      acceptance_state: task.acceptance_state,
      delegated_by: task.delegated_by,
      delegated_by_identity_id: task.delegated_by_identity_id,
      offered_to_agent_id: task.offered_to_agent_id,
      offered_to_identity_id: task.offered_to_identity_id,
      responsible_agent_id: task.responsible_agent_id,
      responsible_identity_id: task.responsible_identity_id,
      point_of_contact_agent_id: task.point_of_contact_agent_id,
      point_of_contact_identity_id: task.point_of_contact_identity_id,
      priority: task.priority,
      response_reason: task.response_reason,
      deferred_until: task.deferred_until,
      lease_until: task.lease_until,
      updated_at: task.updated_at,
    };
  }

  private listCapabilityPresenceOverlays(): CapabilityPresenceOverlay[] {
    return this.listActiveAgents().map((snapshot) => ({
      ...snapshot,
      updated_at: snapshot.last_seen_at,
    }));
  }

  private async upsertCapabilityState(state: AgentSessionState): Promise<void> {
    const manifest = await this.loadCapabilityRegistryManifest(state.last_seen_at);
    const { changed } = upsertCapabilityProfile(manifest, {
      agentId: state.agent_id,
      stableAgentIdentityId: state.stable_agent_identity_id,
      displayName: state.name,
      presence: state.presence,
      joinedAt: state.joined_at,
      lastSeenAt: state.last_seen_at,
      updatedAt: state.last_seen_at,
    });
    if (!changed) {
      return;
    }
    await this.ctx.storage.put(ROOM_CAPABILITY_REGISTRY_KEY, manifest);
  }

  private async applyCapabilityMutation(
    agentState: AgentSessionState,
    params: {
      patch: Parameters<typeof applyCapabilityProfilePatch>[1]["patch"];
      timestamp: string;
    },
  ) {
    const manifest = await this.loadCapabilityRegistryManifest(params.timestamp);
    const result = applyCapabilityProfilePatch(manifest, {
      agentId: agentState.agent_id,
      stableAgentIdentityId: agentState.stable_agent_identity_id,
      displayName: agentState.name,
      presence: agentState.presence,
      joinedAt: agentState.joined_at,
      lastSeenAt: agentState.last_seen_at,
      updatedAt: params.timestamp,
      patch: params.patch,
    });
    if (result.changed) {
      await this.ctx.storage.put(ROOM_CAPABILITY_REGISTRY_KEY, manifest);
    }
    return result;
  }

  private async removeStoredCapabilityProfile(
    agentId: string,
    timestamp: string,
  ): Promise<boolean> {
    const manifest = await this.loadCapabilityRegistryManifest(timestamp);
    const changed = removeCapabilityProfile(manifest, agentId, timestamp);
    if (changed) {
      await this.ctx.storage.put(ROOM_CAPABILITY_REGISTRY_KEY, manifest);
    }
    return changed;
  }

  private async broadcastCapabilityChange(
    sender: WebSocket,
    params: {
      agentState: AgentSessionState;
      timestamp: string;
      event: "capability_updated" | "capability_removed";
      changed: boolean;
    },
  ): Promise<number | null> {
    if (!params.changed) {
      return null;
    }
    const sequence = await this.nextSequence();
    this.broadcast(sender, JSON.stringify(createRoomEvent({
      roomId: params.agentState.room_id,
      sequence,
      timestamp: params.timestamp,
      event: params.event,
      agentId: params.agentState.agent_id,
      stableAgentIdentityId: params.agentState.stable_agent_identity_id,
      name: params.agentState.name,
      presence: params.agentState.presence,
      joinedAt: params.agentState.joined_at,
      lastSeenAt: params.agentState.last_seen_at,
    })));
    return sequence;
  }

  private async maybeCheckpointSessionState(ws: WebSocket, params: {
    previousState: AgentSessionState;
    nextState: AgentSessionState;
    force?: boolean;
  }): Promise<AgentSessionState> {
    if (!shouldCheckpointPresence({
      lastCheckpointAt: params.nextState.checkpointed_at,
      nextLastSeenAt: params.nextState.last_seen_at,
      nextPresence: params.nextState.presence,
      previousPresence: params.previousState.presence,
      force: params.force,
    })) {
      return params.nextState;
    }

    if (!params.force) {
      await this.scheduleTransientCheckpoint(params.nextState.last_seen_at);
      return params.nextState;
    }

    const persistedState: AgentSessionState = {
      ...params.nextState,
      checkpointed_at: params.nextState.last_seen_at,
    };
    this.writeAgentState(ws, persistedState);
    await this.storeSessionState(persistedState);
    return persistedState;
  }

  private async ensureActiveSessionsHydrated(now: string): Promise<void> {
    if (!shouldHydrateActiveSessions({
      lastHydratedAt: this.lastHydratedAt,
      now,
    })) {
      return;
    }

    let shouldScheduleCheckpoint = false;
    for (const ws of this.ctx.getWebSockets()) {
      const previousState = this.readAgentState(ws);
      const nextState = toHydratedPresenceState(previousState, now);
      this.writeAgentState(ws, nextState);
      shouldScheduleCheckpoint ||= shouldCheckpointPresence({
        lastCheckpointAt: nextState.checkpointed_at,
        nextLastSeenAt: nextState.last_seen_at,
        nextPresence: nextState.presence,
        previousPresence: previousState.presence,
      });
    }

    if (shouldScheduleCheckpoint) {
      await this.scheduleTransientCheckpoint(now);
    }

    this.lastHydratedAt = now;
  }

  private async scheduleTransientCheckpoint(now: string): Promise<void> {
    const dueAt = Date.parse(now) + TRANSIENT_CHECKPOINT_BATCH_DELAY_SECONDS * 1000;
    const existingAlarm = await this.ctx.storage.getAlarm();
    if (existingAlarm !== null && existingAlarm <= dueAt) {
      return;
    }
    await this.ctx.storage.setAlarm(dueAt);
  }

  private async flushTransientSessionCheckpoints(now: string): Promise<void> {
    const manifest = await this.loadSessionCheckpointManifest();
    let changed = false;

    for (const ws of this.ctx.getWebSockets()) {
      const state = this.readAgentState(ws);
      const stored = manifest.sessions[state.session_id];
      const shouldPersist =
        !stored ||
        shouldCheckpointPresence({
          lastCheckpointAt: stored?.checkpointed_at ?? state.checkpointed_at,
          nextLastSeenAt: state.last_seen_at,
          nextPresence: state.presence,
          previousPresence: stored?.presence ?? state.presence,
        });

      if (!shouldPersist) {
        continue;
      }

      const persistedState: AgentSessionState = {
        ...state,
        checkpointed_at: state.last_seen_at,
      };
      this.writeAgentState(ws, persistedState);
      manifest.sessions[persistedState.session_id] = this.toStoredSession(persistedState);
      changed = true;
    }

    changed = this.pruneExpiredSessionCheckpoints(manifest, now) || changed;

    if (!changed) {
      return;
    }

    manifest.updated_at = now;
    await this.ctx.storage.put(ROOM_SESSION_CHECKPOINTS_KEY, manifest);
  }

  private async handleTaskRequest(url: URL): Promise<Response> {
    const manifest = await this.loadTaskRegistryManifest();
    const segments = url.pathname.split("/").filter(Boolean);
    const taskId = segments[1] ? decodeURIComponent(segments[1]) : undefined;
    const roomId = url.searchParams.get("room_id") || "unknown";

    if (!taskId) {
      const tasks = listTasks(manifest);
      return Response.json({
        success: true,
        room_id: roomId,
        updated_at: manifest.updated_at,
        count: tasks.length,
        tasks,
      });
    }

    const task = getTask(manifest, taskId);
    if (!task) {
      return Response.json(
        { success: false, error: `Task '${taskId}' not found.` },
        { status: 404 },
      );
    }

    return Response.json({
      success: true,
      room_id: roomId,
      updated_at: manifest.updated_at,
      task,
    });
  }

  private async handleCapabilityRequest(url: URL): Promise<Response> {
    const manifest = await this.loadCapabilityRegistryManifest();
    const profiles = listCapabilityProfiles(
      manifest,
      this.listCapabilityPresenceOverlays(),
    );
    const segments = url.pathname.split("/").filter(Boolean);
    const collection = segments[1];
    const entryId = segments[2] ? decodeURIComponent(segments[2]) : undefined;
    const roomId = url.searchParams.get("room_id") || "unknown";

    if (collection === "agents" && !entryId) {
      return Response.json({
        success: true,
        room_id: roomId,
        updated_at: manifest.updated_at,
        count: profiles.length,
        agents: profiles,
      });
    }

    if (collection === "agents" && entryId) {
      const agent = profiles.find((profile) => profile.agent_id === entryId);
      if (!agent) {
        return Response.json(
          { success: false, error: `Capability profile '${entryId}' not found.` },
          { status: 404 },
        );
      }
      return Response.json({
        success: true,
        room_id: roomId,
        updated_at: manifest.updated_at,
        agent,
      });
    }

    const skills = buildCapabilitySkillIndex(profiles);
    if (collection === "skills" && !entryId) {
      return Response.json({
        success: true,
        room_id: roomId,
        updated_at: manifest.updated_at,
        count: skills.length,
        skills,
      });
    }

    if (collection === "skills" && entryId) {
      const skill = skills.find((entry) => entry.skill_id === entryId);
      if (!skill) {
        return Response.json(
          { success: false, error: `Skill '${entryId}' not found.` },
          { status: 404 },
        );
      }
      return Response.json({
        success: true,
        room_id: roomId,
        updated_at: manifest.updated_at,
        skill,
      });
    }

    return new Response("Not Found", { status: 404 });
  }

  private findActiveSession(
    sessionId: string,
    excludedWs?: WebSocket,
  ): AgentSessionState | null {
    if (!sessionId) {
      return null;
    }

    for (const ws of this.ctx.getWebSockets()) {
      if (ws === excludedWs) {
        continue;
      }
      const state = this.readAgentState(ws);
      if (state.session_id === sessionId) {
        return state;
      }
    }

    return null;
  }

  private toStoredSession(state: AgentSessionState): StoredRoomSession {
    return {
      session_id: state.session_id,
      agent_id: state.agent_id,
      stable_agent_identity_id: state.stable_agent_identity_id,
      name: state.name,
      joined_at: state.joined_at,
      last_seen_at: state.last_seen_at,
      presence: state.presence,
      checkpointed_at: state.checkpointed_at,
    };
  }

  private async loadSessionCheckpointManifest(): Promise<RoomSessionCheckpointManifest> {
    const stored =
      (await this.ctx.storage.get<RoomSessionCheckpointManifest>(ROOM_SESSION_CHECKPOINTS_KEY)) ??
      {
        updated_at: new Date().toISOString(),
        sessions: {},
      };

    return {
      updated_at: typeof stored.updated_at === "string"
        ? stored.updated_at
        : new Date().toISOString(),
      sessions: typeof stored.sessions === "object" && stored.sessions
        ? stored.sessions
        : {},
    };
  }

  private pruneExpiredSessionCheckpoints(
    manifest: RoomSessionCheckpointManifest,
    now: string,
  ): boolean {
    let changed = false;
    for (const [sessionId, stored] of Object.entries(manifest.sessions)) {
      if (this.findActiveSession(sessionId)) {
        continue;
      }
      if (shouldPruneSessionCheckpoint({ session: stored, now })) {
        delete manifest.sessions[sessionId];
        changed = true;
      }
    }
    return changed;
  }

  private async getStoredSession(sessionId: string, now: string): Promise<StoredRoomSession | null> {
    const manifest = await this.loadSessionCheckpointManifest();
    const fromManifest = manifest.sessions[sessionId];
    if (fromManifest) {
      if (shouldPruneSessionCheckpoint({ session: fromManifest, now })) {
        delete manifest.sessions[sessionId];
        manifest.updated_at = now;
        await this.ctx.storage.put(ROOM_SESSION_CHECKPOINTS_KEY, manifest);
        return null;
      }
      return fromManifest;
    }

    const legacy = await this.ctx.storage.get<StoredRoomSession>(`session:${sessionId}`);
    if (!legacy) {
      return null;
    }

    if (shouldPruneSessionCheckpoint({ session: legacy, now })) {
      return null;
    }

    return legacy;
  }

  private async resolveSession(params: {
    sessionId: string;
    agentName: string;
    now: string;
  }): Promise<{ agentId: string; joinedAt: string; reconnected: boolean }> {
    const active = this.findActiveSession(params.sessionId);
    if (active) {
      return {
        agentId: active.agent_id,
        joinedAt: active.joined_at,
        reconnected: true,
      };
    }

    const stored = await this.getStoredSession(params.sessionId, params.now);

    if (
      stored &&
      shouldResumeSession({ lastSeenAt: stored.last_seen_at, now: params.now })
    ) {
      return {
        agentId: stored.agent_id,
        joinedAt: stored.joined_at,
        reconnected: true,
      };
    }

    return {
      agentId: generateId(8),
      joinedAt: params.now,
      reconnected: false,
    };
  }

  // Broadcast ke semua kecuali sender
  private broadcast(sender: WebSocket | null, message: string): number {
    let delivered = 0;
    for (const ws of this.ctx.getWebSockets()) {
      if (ws !== sender) {
        try {
          ws.send(message);
          delivered += 1;
        } catch {}
      }
    }
    return delivered;
  }
}

// ─── Durable Object: Registry ─────────────────────────────────────────────────
// Menyimpan metadata room (nama, token join, kepemilikan)

export class AgentLinkRegistry extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    const url    = new URL(request.url);
    const action = url.pathname.replace("/", "");

    // Statistik agregat saja — daftar room tidak pernah diekspos
    if (action === "list") {
      const all = await this.ctx.storage.list<RoomMeta>({ prefix: "room:" });
      return Response.json(summarizeRoomActivity(all.values()));
    }

    // Buang room warisan mode publik yang sudah tidak bisa dimasuki siapa pun.
    // Default dry-run: menghapus hanya terjadi bila confirm=true dikirim.
    if (action === "prune-unjoinable" && request.method === "POST") {
      const confirm = url.searchParams.get("confirm") === "true";
      const all = await this.ctx.storage.list<RoomMeta>({ prefix: "room:" });

      const doomed: { key: string; room_id: string; created_at: string }[] = [];
      for (const [key, room] of all) {
        if (isUnjoinableRoom(room)) {
          doomed.push({ key, room_id: room.room_id, created_at: room.created_at });
        }
      }

      let deleted = 0;
      if (confirm) {
        // storage.delete menerima maksimal 128 key per panggilan.
        for (let i = 0; i < doomed.length; i += 128) {
          const chunk = doomed.slice(i, i + 128).map((entry) => entry.key);
          deleted += await this.ctx.storage.delete(chunk);
        }
      }

      return Response.json({
        ok: true,
        dry_run: !confirm,
        total_rooms_before: all.size,
        unjoinable_found: doomed.length,
        joinable_kept: all.size - doomed.length,
        deleted,
        // Contoh saja, dan hanya room mati — tidak ada token di bentuk ini.
        sample: doomed.slice(0, 10).map((entry) => ({
          room_id: entry.room_id,
          created_at: entry.created_at,
        })),
      });
    }

    // Register room baru
    if (action === "register" && request.method === "POST") {
      const data = await request.json() as RoomMeta;
      const roleState = normalizeRoomRoleState(data);
      const room: RoomMeta = {
        ...data,
        owner_stable_identity_id: roleState.owner_stable_identity_id,
        admin_stable_identity_ids: roleState.admin_stable_identity_ids,
      };
      await this.ctx.storage.put(`room:${room.room_id}`, room);
      return Response.json({ ok: true });
    }

    // Room melaporkan jumlah agent aktifnya. Nilai absolut, bukan selisih, supaya
    // laporan berikutnya selalu mengoreksi yang sebelumnya dan galat tidak menumpuk.
    if (action === "set-agent-count" && request.method === "POST") {
      const body = await request.json() as { room_id?: string; agent_count?: number };
      const roomId = typeof body.room_id === "string" ? body.room_id : "";
      const count = body.agent_count;
      if (!roomId || !Number.isInteger(count) || (count as number) < 0) {
        return Response.json(
          { ok: false, error: "room_id and agent_count (an integer >= 0) are required." },
          { status: 400 },
        );
      }

      const room = await this.ctx.storage.get<RoomMeta>(`room:${roomId}`);
      if (!room) {
        // Room sudah dihapus (mis. lewat prune). Bukan error yang perlu diulang.
        return Response.json({ ok: true, room_missing: true });
      }
      if (room.agent_count === count) {
        return Response.json({ ok: true, unchanged: true });
      }

      await this.ctx.storage.put(`room:${roomId}`, { ...room, agent_count: count });
      return Response.json({ ok: true, agent_count: count });
    }

    if (action === "room") {
      const room_id = url.searchParams.get("room_id") || "";
      const room = await this.ctx.storage.get<RoomMeta>(`room:${room_id}`);
      if (!room) {
        return Response.json(
          { ok: false, error: `Room '${room_id}' not found.` },
          { status: 404 },
        );
      }
      return Response.json({ ok: true, room });
    }

    // Check room + token validity. Semua room private: token selalu wajib.
    if (action === "check") {
      const room_id = url.searchParams.get("room_id") || "";
      const token   = url.searchParams.get("token") || "";

      // Diperiksa sebelum room dibaca: ini soal bentuk permintaan, bukan soal room,
      // jadi menjawabnya spesifik tidak membocorkan apa pun tentang room mana pun.
      if (!token) {
        return Response.json({ ok: false, error: "A token is required to join a room." });
      }

      const room = await this.ctx.storage.get<RoomMeta>(`room:${room_id}`);

      // Satu pesan untuk semua kegagalan berikutnya — room tidak ada, room warisan
      // publik yang tersimpan tanpa token, atau token salah. Membedakannya akan
      // memberi tahu penebak bahwa sebuah Room ID itu ada tanpa perlu tokennya,
      // padahal justru itu yang kita rahasiakan.
      if (!room || !room.token || !timingSafeEqual(token, room.token)) {
        return Response.json({ ok: false, error: "Room ID or token is wrong." });
      }

      return Response.json({ ok: true, room });
    }

    if ((action === "grant-admin" || action === "revoke-admin") && request.method === "POST") {
      const {
        room_id,
        actor_stable_identity_id,
        target_stable_identity_id,
      } = await request.json() as {
        room_id?: string;
        actor_stable_identity_id?: string;
        target_stable_identity_id?: string;
      };
      const roomId = typeof room_id === "string" ? room_id : "";
      const room = await this.ctx.storage.get<RoomMeta>(`room:${roomId}`);
      if (!room) {
        return Response.json(
          { ok: false, error: `Room '${roomId}' not found.` },
          { status: 404 },
        );
      }

      const mutation = action === "grant-admin"
        ? grantRoomAdmin(room, {
          actorStableIdentityId: actor_stable_identity_id,
          targetStableIdentityId: target_stable_identity_id,
        })
        : revokeRoomAdmin(room, {
          actorStableIdentityId: actor_stable_identity_id,
          targetStableIdentityId: target_stable_identity_id,
        });

      if (!mutation.ok) {
        return Response.json(
          {
            ok: false,
            changed: false,
            error: mutation.error || "Unauthorized",
            role_state: mutation.role_state,
          },
          { status: 403 },
        );
      }

      if (mutation.changed) {
        const nextRoom: RoomMeta = {
          ...room,
          owner_stable_identity_id: mutation.role_state.owner_stable_identity_id,
          admin_stable_identity_ids: mutation.role_state.admin_stable_identity_ids,
        };
        await this.ctx.storage.put(`room:${roomId}`, nextRoom);
      }

      return Response.json({
        ok: true,
        changed: mutation.changed,
        role_state: mutation.role_state,
      });
    }

    // Delete room (cleanup)
    if (action === "delete" && request.method === "POST") {
      const { room_id } = await request.json() as { room_id: string };
      await this.ctx.storage.delete(`room:${room_id}`);
      return Response.json({ ok: true });
    }

    return new Response("Not Found", { status: 404 });
  }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Ambil token room dari header `X-Room-Token` lebih dulu, baru fallback ke query
 * `?token=`. Header didahulukan supaya klien browser tidak perlu menaruh kunci
 * di URL, yang akan bocor ke history, header Referer, dan log akses.
 * Query param dipertahankan demi kompatibilitas MCP client yang sudah beredar.
 */
function readRoomToken(request: Request, url: URL): string {
  return request.headers.get("X-Room-Token") || url.searchParams.get("token") || "";
}

/**
 * Salin hanya key yang benar-benar ada di pesan klien.
 *
 * Validator capability membedakan "tidak dikirim" dari "dikirim bernilai salah"
 * lewat `"key" in input`. Menyusun objek yang menyebut semua key secara eksplisit
 * membuat pembedaan itu runtuh: key yang tidak dikirim tetap ada dengan nilai
 * undefined, lalu ditolak sebagai nilai tidak valid — sehingga patch parsial,
 * yang justru perilaku utama tool ini, tidak pernah bisa lolos.
 */
function pickProvided(
  source: Record<string, unknown>,
  keys: readonly string[],
): Record<string, unknown> {
  const picked: Record<string, unknown> = {};
  for (const key of keys) {
    if (key in source) {
      picked[key] = source[key];
    }
  }
  return picked;
}

/**
 * Perbandingan yang waktunya tidak bergantung pada posisi karakter pertama yang
 * berbeda, supaya secret admin tidak bisa ditebak bertahap lewat pengukuran waktu.
 */
function timingSafeEqual(provided: string, expected: string): boolean {
  if (provided.length !== expected.length) {
    return false;
  }
  let diff = 0;
  for (let i = 0; i < provided.length; i++) {
    diff |= provided.charCodeAt(i) ^ expected.charCodeAt(i);
  }
  return diff === 0;
}

function generateId(len: number): string {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  return Array.from(bytes, b => chars[b % chars.length]).join("");
}

function sanitizeStableAgentIdentityId(value: string | null): string | undefined {
  if (!value) {
    return undefined;
  }
  const normalized = value.trim();
  if (!normalized || normalized.length > 128) {
    return undefined;
  }
  return /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(normalized)
    ? normalized
    : undefined;
}
