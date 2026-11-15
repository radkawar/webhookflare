import PostalMime from 'postal-mime';
import { DurableObject } from 'cloudflare:workers';

/**
 * WebhookBin - A webhook tester powered by Cloudflare Durable Objects
 *
 * Each webhook endpoint is a separate Durable Object instance that:
 * - Stores incoming webhook requests in SQLite
 * - Maintains WebSocket connections for real-time updates
 * - Broadcasts new requests to all connected viewers
 */

// ============================================================================
// TYPES
// ============================================================================

interface WebhookRequest {
	id: number;
	timestamp: string;
	method: string;
	headers: string;
	body: string;
	query: string;
	metadata: string;
}

interface CapturedMetadata {
	city?: string | unknown;
	postalCode?: string | unknown;
	region?: string | unknown;
	regionCode?: string | unknown;
	country?: string | unknown;
	continent?: string | unknown;
	timezone?: string | unknown;
	latitude?: number | unknown;
	longitude?: number | unknown;
	asOrganization?: string | unknown;
	userIP?: string | null;
}

// ============================================================================
// CONSTANTS
// ============================================================================

const MINUTE_MS = 60 * 1000;
const HOUR_MS = 60 * MINUTE_MS;
const DAY_MS = 24 * HOUR_MS;

const CONFIG = {
	DEFAULT_TTL_MS: 6 * HOUR_MS,
	MIN_TTL_MS: 15 * MINUTE_MS,
	MAX_TTL_MS: 30 * DAY_MS,
};

const TTL_PRESETS: Record<string, number> = {
	'15m': 15 * MINUTE_MS,
	'1h': HOUR_MS,
	'6h': 6 * HOUR_MS,
	'24h': DAY_MS,
	'7d': 7 * DAY_MS,
	'30d': 30 * DAY_MS,
};

const CORS_HEADERS = {
	'Access-Control-Allow-Origin': '*',
	'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
	'Access-Control-Allow-Headers': 'Content-Type',
	'Access-Control-Expose-Headers': 'X-Expires-At, X-TTL-Ms',
};

const ROUTES = {
	API_NEW: '/api/new',
	API_BIN: '/api/bin/',
	WEBSOCKET: '/ws/',
	WEBHOOK: '/hook/',
	INIT: '/init',
};

const RESPONSES = {
	INVALID_BIN_ID: 'Invalid bin ID',
	INVALID_WEBHOOK_ID: 'Invalid webhook ID',
	EXPECTED_WEBSOCKET: 'Expected WebSocket',
	INITIALIZED: 'initialized',
	WEBHOOK_CAPTURED: 'Webhook captured',
};

/** WebhookBin Durable Object - stores and broadcasts webhook requests */
export class WebhookBin extends DurableObject {
	private sessions: Set<WebSocket>;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.sessions = new Set();

		// Initialize SQLite table for storing webhook requests
		this.ctx.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS requests (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				timestamp TEXT NOT NULL,
				method TEXT NOT NULL,
				headers TEXT NOT NULL,
				body TEXT,
				query TEXT,
				metadata TEXT
			)
		`);
	}

	// ========================================================================
	// Storage & Scheduling
	// ========================================================================

	/**
	 * Ensure this Durable Object has a createdAt timestamp, a stored TTL, and a scheduled alarm.
	 * TTL is fixed at first init. Missing ttlMs means a bin created before configurable expiry (6 hours).
	 */
	private async ensureScheduled(ttlMs?: number): Promise<{ createdAt: number; ttlMs: number; expiresAt: number }> {
		const existingCreatedAt = (await this.ctx.storage.get('createdAt')) as number | undefined;
		if (!existingCreatedAt) {
			const now = Date.now();
			const ttl = clampTtl(ttlMs ?? CONFIG.DEFAULT_TTL_MS);
			await this.ctx.storage.put('createdAt', now);
			await this.ctx.storage.put('ttlMs', ttl);
			await this.ctx.storage.setAlarm(now + ttl);
			return { createdAt: now, ttlMs: ttl, expiresAt: now + ttl };
		}

		const storedTtl = (await this.ctx.storage.get('ttlMs')) as number | undefined;
		const ttl = storedTtl ?? CONFIG.DEFAULT_TTL_MS;
		const expiresAt = existingCreatedAt + ttl;
		const alarm = await this.ctx.storage.getAlarm();
		if (alarm == null) {
			await this.ctx.storage.setAlarm(Math.max(expiresAt, Date.now() + 1000));
		}
		return { createdAt: existingCreatedAt, ttlMs: ttl, expiresAt };
	}

	private async readMeta(): Promise<{ createdAt: number | null; ttlMs: number; expiresAt: number | null }> {
		const createdAt = (await this.ctx.storage.get('createdAt')) as number | undefined;
		const storedTtl = (await this.ctx.storage.get('ttlMs')) as number | undefined;
		const ttlMs = storedTtl ?? CONFIG.DEFAULT_TTL_MS;
		if (!createdAt) {
			return { createdAt: null, ttlMs, expiresAt: null };
		}
		return { createdAt, ttlMs, expiresAt: createdAt + ttlMs };
	}

	// ========================================================================
	// Data Operations
	// ========================================================================

	/**
	 * Store a webhook request and broadcast to all connected WebSocket clients
	 */
	async captureRequest(
		method: string,
		headers: Record<string, string>,
		body: string,
		query: string,
		metadata: string
	): Promise<number> {
		const timestamp = new Date().toISOString();

		// Store in SQLite
		const result = this.ctx.storage.sql
			.exec(
				`INSERT INTO requests (timestamp, method, headers, body, query, metadata) 
			 VALUES (?, ?, ?, ?, ?, ?) 
			 RETURNING id`,
				timestamp,
				method,
				JSON.stringify(headers),
				body,
				query,
				metadata
			)
			.one() as { id: number };

		// Broadcast to all WebSocket connections
		const request: WebhookRequest = {
			id: result.id,
			timestamp,
			method,
			headers: JSON.stringify(headers),
			body,
			query,
			metadata,
		};

		this.broadcastMessage({ type: 'new_request', data: request });

		return result.id;
	}

	/**
	 * Get all stored requests for this webhook bin
	 */
	async getRequests(): Promise<WebhookRequest[]> {
		const results = this.ctx.storage.sql
			.exec(
				`SELECT id, timestamp, method, headers, body, query, metadata
			 FROM requests
			 ORDER BY id DESC`
			)
			.toArray() as unknown as WebhookRequest[];

		return results;
	}

	// ========================================================================
	// WebSocket Handling
	// ========================================================================

	/**
	 * Handle WebSocket connections for real-time updates and lightweight init
	 */
	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);

		if (url.pathname === ROUTES.INIT) {
			const raw = url.searchParams.get('ttl');
			const ttlMs = raw == null ? undefined : Number(raw);
			if (raw != null && !Number.isFinite(ttlMs)) {
				return new Response('Invalid ttl', { status: 400 });
			}
			const meta = await this.ensureScheduled(ttlMs);
			return new Response(JSON.stringify(meta), {
				status: 200,
				headers: { 'Content-Type': 'application/json' },
			});
		}

		if (url.pathname === '/meta') {
			return new Response(JSON.stringify(await this.readMeta()), {
				status: 200,
				headers: { 'Content-Type': 'application/json' },
			});
		}

		// Handle WebSocket upgrade
		if (request.headers.get('Upgrade') === 'websocket') {
			return this.handleWebSocketConnection(request);
		}

		return new Response(RESPONSES.EXPECTED_WEBSOCKET, { status: 400 });
	}

	/**
	 * Handle a new WebSocket connection
	 */
	private async handleWebSocketConnection(request: Request): Promise<Response> {
		const meta = await this.ensureScheduled();

		const pair = new WebSocketPair();
		const [client, server] = Object.values(pair);

		server.accept();
		this.sessions.add(server);

		const requests = await this.getRequests();
		server.send(JSON.stringify({
			type: 'initial_data',
			data: requests,
			expiresAt: meta.expiresAt,
			ttlMs: meta.ttlMs,
		}));

		// Handle close event
		server.addEventListener('close', () => {
			this.sessions.delete(server);
		});

		// Handle error event
		server.addEventListener('error', () => {
			this.sessions.delete(server);
		});

		return new Response(null, { status: 101, webSocket: client });
	}

	/**
	 * Broadcast message to all connected WebSocket clients
	 */
	private broadcastMessage(data: unknown): void {
		this.broadcast(JSON.stringify(data));
	}

	/**
	 * Send a message to all connected WebSocket clients
	 */
	private broadcast(message: string): void {
		this.sessions.forEach((session) => {
			try {
				session.send(message);
			} catch (err) {
				// Client disconnected, remove from sessions
				this.sessions.delete(session);
			}
		});
	}

	// ========================================================================
	// Lifecycle
	// ========================================================================

	/**
	 * Alarm handler - called when the object's alarm fires (TTL expiration)
	 */
	async alarm(): Promise<void> {
		try {
			// Broadcast deletion event to all connected clients
			this.broadcastMessage({ type: 'deleted', reason: 'expired' });

			// Close all WebSocket connections
			this.sessions.forEach((session) => {
				try {
					session.send(JSON.stringify({ type: 'deleted', reason: 'expired' }));
					session.close();
				} catch (err) {
					// Ignore errors during close
				}
			});
			this.sessions.clear();

			// Remove all storage (including sqlite state) to free space
			await this.ctx.storage.deleteAll();
		} catch (err) {
			// Let the system retry if this throws; alarms have automatic retries
			throw err;
		}
	}
}


// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Generate a random webhook ID (8 random alphanumeric characters)
 */
function generateWebhookId(): string {
	return Array.from({ length: 8 }, () => Math.random().toString(36).charAt(2)).join('');
}

/**
 * Extract ID from a path like /ws/{id} or /api/bin/{id}
 */
function extractIdFromPath(path: string, position: number = 2): string | null {
	const parts = path.split('/');
	return parts[position] || null;
}

/**
 * Create a JSON response with CORS headers
 */
function createJsonResponse(
	data: unknown,
	status: number = 200,
	extraHeaders: Record<string, string> = {}
): Response {
	return new Response(JSON.stringify(data), {
		status,
		headers: {
			'Content-Type': 'application/json',
			...CORS_HEADERS,
			...extraHeaders,
		},
	});
}

function clampTtl(ttlMs: number): number {
	if (!Number.isFinite(ttlMs)) return CONFIG.DEFAULT_TTL_MS;
	return Math.min(CONFIG.MAX_TTL_MS, Math.max(CONFIG.MIN_TTL_MS, Math.floor(ttlMs)));
}

function parseTtl(raw: string | null): { ok: true; ttlMs: number } | { ok: false; error: string } {
	if (raw == null || raw.trim() === '') return { ok: true, ttlMs: CONFIG.DEFAULT_TTL_MS };
	const key = raw.trim().toLowerCase();
	const preset = TTL_PRESETS[key];
	let ttlMs: number | undefined = preset;
	if (ttlMs == null && /^\d+$/.test(key)) ttlMs = Number(key) * 1000;
	const duration = /^(\d+)(m|h|d)$/.exec(key);
	if (ttlMs == null && duration) {
		const amount = Number(duration[1]);
		ttlMs = amount * (duration[2] === 'm' ? MINUTE_MS : duration[2] === 'h' ? HOUR_MS : DAY_MS);
	}
	if (ttlMs == null || !Number.isFinite(ttlMs)) {
		return {
			ok: false,
			error: 'Invalid ttl. Use 15m, 1h, 6h, 24h, 7d, 30d, or seconds from 900 to 2592000.',
		};
	}
	if (ttlMs < CONFIG.MIN_TTL_MS || ttlMs > CONFIG.MAX_TTL_MS) {
		return { ok: false, error: 'ttl must be between 15 minutes and 30 days' };
	}
	return { ok: true, ttlMs };
}

/**
 * Create an error response with CORS headers
 */
function createErrorResponse(message: string, status: number = 400): Response {
	return createJsonResponse({ error: message }, status);
}

/**
 * Create a CORS preflight response
 */
function createCorsPreflightResponse(): Response {
	return new Response(null, {
		status: 204,
		headers: CORS_HEADERS,
	});
}

/**
 * Extract all headers from a request as a record
 */
function extractHeaders(request: Request): Record<string, string> {
	const headers: Record<string, string> = {};
	request.headers.forEach((value, key) => {
		headers[key] = value;
	});
	return headers;
}

/**
 * Build metadata object from Cloudflare request metadata
 */
function buildMetadata(request: Request): CapturedMetadata {
	return {
		city: request.cf?.city,
		postalCode: request.cf?.postalCode,
		region: request.cf?.region,
		regionCode: request.cf?.regionCode,
		country: request.cf?.country,
		continent: request.cf?.continent,
		timezone: request.cf?.timezone,
		latitude: request.cf?.latitude,
		longitude: request.cf?.longitude,
		asOrganization: request.cf?.asOrganization,
		userIP: request.headers.get('CF-Connecting-IP') || undefined,
	};
}

// ============================================================================
// ROUTE HANDLERS
// ============================================================================

/**
 * Handle: POST /api/new
 * Generate a new webhook ID and initialize its Durable Object
 */
async function handleCreateWebhook(env: Env, request: Request): Promise<Response> {
	if (!env.MY_DURABLE_OBJECT) {
		console.error('MY_DURABLE_OBJECT binding is not configured');
		return createErrorResponse('Service unavailable - Durable Object not configured', 503);
	}

	const parsed = parseTtl(new URL(request.url).searchParams.get('ttl'));
	if (!parsed.ok) return createErrorResponse(parsed.error);

	const id = generateWebhookId();
	const stub = env.MY_DURABLE_OBJECT.get(
		env.MY_DURABLE_OBJECT.idFromName(id)
	) as DurableObjectStub<WebhookBin>;

	try {
		const initResponse = await stub.fetch(new Request(`https://internal/init?ttl=${parsed.ttlMs}`));
		if (!initResponse.ok) {
			return createErrorResponse('Failed to initialize webhook', 500);
		}
		const meta = (await initResponse.json()) as { ttlMs: number; expiresAt: number };
		return createJsonResponse({ id, ttlMs: meta.ttlMs, expiresAt: meta.expiresAt });
	} catch (err) {
		console.error('DO init failed', err);
		return createErrorResponse('Failed to initialize webhook', 500);
	}
}

/**
 * Handle: GET /api/bin/{id}
 * Retrieve all captured requests for a specific webhook bin
 */
async function handleGetBinRequests(env: Env, id: string): Promise<Response> {
	if (!id) {
		return createErrorResponse(RESPONSES.INVALID_BIN_ID);
	}

	if (!env.MY_DURABLE_OBJECT) {
		console.error('MY_DURABLE_OBJECT binding is not configured');
		return createErrorResponse('Service unavailable - Durable Object not configured', 503);
	}

	const stub = env.MY_DURABLE_OBJECT.get(
		env.MY_DURABLE_OBJECT.idFromName(id)
	) as DurableObjectStub<WebhookBin>;

	try {
		const [requests, metaResponse] = await Promise.all([
			stub.getRequests(),
			stub.fetch(new Request('https://internal/meta')),
		]);
		if (!metaResponse.ok) return createErrorResponse('Failed to retrieve requests', 500);
		const meta = (await metaResponse.json()) as { ttlMs: number; expiresAt: number | null };
		const extraHeaders: Record<string, string> = { 'X-TTL-Ms': String(meta.ttlMs) };
		if (meta.expiresAt != null) extraHeaders['X-Expires-At'] = String(meta.expiresAt);
		return createJsonResponse(requests, 200, extraHeaders);
	} catch (err) {
		console.error(`Failed to get requests for bin ${id}:`, err);
		return createErrorResponse('Failed to retrieve requests', 500);
	}
}

/**
 * Handle: WS /ws/{id}
 * Establish WebSocket connection for real-time updates
 */
async function handleWebSocketConnection(env: Env, id: string, request: Request): Promise<Response> {
	if (!id) {
		return createErrorResponse(RESPONSES.INVALID_BIN_ID);
	}

	if (!env.MY_DURABLE_OBJECT) {
		console.error('MY_DURABLE_OBJECT binding is not configured');
		return createErrorResponse('Service unavailable - Durable Object not configured', 503);
	}

	const stub = env.MY_DURABLE_OBJECT.get(
		env.MY_DURABLE_OBJECT.idFromName(id)
	) as DurableObjectStub<WebhookBin>;

	try {
		return await stub.fetch(request);
	} catch (err) {
		console.error(`Failed to establish WebSocket for bin ${id}:`, err);
		return createErrorResponse('Failed to establish WebSocket connection', 500);
	}
}

/**
 * Handle: POST /hook/{id}
 * Capture an incoming webhook request
 */
async function handleWebhookCapture(
	env: Env,
	id: string,
	request: Request
): Promise<Response> {
	if (!id) {
		return createErrorResponse(RESPONSES.INVALID_WEBHOOK_ID);
	}

	try {
		// Validate that the Durable Object binding exists
		if (!env.MY_DURABLE_OBJECT) {
			console.error('MY_DURABLE_OBJECT binding is not configured');
			return createErrorResponse('Service unavailable - Durable Object not configured', 503);
		}

		// Apply rate limiting to the webhook endpoint
		if (env.WEBHOOK_RATE_LIMITER) {
			const { success } = await env.WEBHOOK_RATE_LIMITER.limit({ key: id });
			if (!success) {
				return createErrorResponse(`Rate limit exceeded for webhook ${id}`, 429);
			}
		}

		// Extract request details
		const headers = extractHeaders(request);
		const body = await request.text();
		const query = new URL(request.url).search;
		const metadata = JSON.stringify(buildMetadata(request));

		// Store in Durable Object
		const stub = env.MY_DURABLE_OBJECT.get(
			env.MY_DURABLE_OBJECT.idFromName(id)
		) as DurableObjectStub<WebhookBin>;

		const requestId = await stub.captureRequest(request.method, headers, body, query, metadata);

		return createJsonResponse({
			success: true,
			message: RESPONSES.WEBHOOK_CAPTURED,
			requestId,
		});
	} catch (err) {
		console.error(`Failed to capture webhook for ID ${id}:`, err);
		return createErrorResponse('Failed to capture webhook', 500);
	}
}

const EMAIL_DOMAINS = ['webhook.t-iq.me', 't-iq.me'];
const BIN_ID = /^[a-z0-9]{8}$/;
const EMAIL_PART_LIMIT = 100_000;

function binIdFromRecipient(address: string): string | null {
	const at = address.lastIndexOf('@');
	if (at <= 0) return null;
	const domain = address.slice(at + 1).toLowerCase();
	if (!EMAIL_DOMAINS.includes(domain)) return null;
	let local = address.slice(0, at).toLowerCase();
	const plus = local.indexOf('+');
	if (plus >= 0) local = local.slice(0, plus);
	return BIN_ID.test(local) ? local : null;
}

function clipEmailPart(value: string | undefined | null): string | undefined {
	if (!value) return undefined;
	return value.length > EMAIL_PART_LIMIT ? `${value.slice(0, EMAIL_PART_LIMIT)}\n…truncated` : value;
}

function emailHeaders(headers: Headers): Record<string, string> {
	const record: Record<string, string> = {};
	headers.forEach((value, key) => {
		record[key] = value;
	});
	return record;
}

async function handleInboundEmail(message: ForwardableEmailMessage, env: Env): Promise<void> {
	const id = binIdFromRecipient(message.to);
	if (!id) {
		message.setReject('Unknown webhook address');
		return;
	}
	if (!env.MY_DURABLE_OBJECT) {
		message.setReject('Webhook storage is unavailable');
		return;
	}
	if (env.WEBHOOK_RATE_LIMITER) {
		const { success } = await env.WEBHOOK_RATE_LIMITER.limit({ key: id });
		if (!success) {
			message.setReject('Rate limit exceeded');
			return;
		}
	}

	const stub = env.MY_DURABLE_OBJECT.get(env.MY_DURABLE_OBJECT.idFromName(id)) as DurableObjectStub<WebhookBin>;
	const metaResponse = await stub.fetch(new Request('https://internal/meta'));
	const meta = (await metaResponse.json()) as { createdAt: number | null };
	if (!meta.createdAt) {
		message.setReject('Unknown webhook address');
		return;
	}

	const parsed = await PostalMime.parse(message.raw);
	const body = JSON.stringify({
		from: message.from,
		to: message.to,
		subject: parsed.subject || '',
		text: clipEmailPart(parsed.text),
		html: clipEmailPart(parsed.html),
		attachments: (parsed.attachments || []).map((attachment) => ({
			filename: attachment.filename || '',
			mimeType: attachment.mimeType || '',
			disposition: attachment.disposition || '',
			size: attachment.content ? attachment.content.byteLength || attachment.content.length || 0 : 0,
		})),
	});
	const headers = emailHeaders(message.headers);
	headers.from = message.from;
	headers.to = message.to;
	if (parsed.subject) headers.subject = parsed.subject;
	const metadata = JSON.stringify({
		rawSize: message.rawSize,
		from: message.from,
		authentication: message.headers.get('authentication-results') || undefined,
	});
	await stub.captureRequest('EMAIL', headers, body, '', metadata);
}


// ============================================================================
// MAIN HANDLER
// ============================================================================

export default {
	/**
	 * Worker fetch handler - routes requests to appropriate handlers
	 */
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		try {
			const url = new URL(request.url);
			const path = url.pathname;

			// Handle CORS preflight
			if (request.method === 'OPTIONS') {
				return createCorsPreflightResponse();
			}

			// Route to appropriate handler
			if (path === ROUTES.API_NEW) {
				return handleCreateWebhook(env, request);
			}

			if (path.startsWith(ROUTES.API_BIN)) {
				const id = extractIdFromPath(path, 3);
				return handleGetBinRequests(env, id || '');
			}

			if (path.startsWith(ROUTES.WEBSOCKET)) {
				const id = extractIdFromPath(path, 2);
				return handleWebSocketConnection(env, id || '', request);
			}

			if (path.startsWith(ROUTES.WEBHOOK)) {
				const id = extractIdFromPath(path, 2);
				return handleWebhookCapture(env, id || '', request);
			}

			// Serve static assets for root and other paths
			const assets = (env as any).ASSETS;
			if (assets) {
				return assets.fetch(request);
			}

			// Fallback: return 404 if no ASSETS binding
			return createErrorResponse('Not found', 404);
		} catch (err) {
			console.error('Unhandled error in fetch handler:', err);
			return createErrorResponse('Internal server error', 500);
		}
	},
	async email(message, env) {
		await handleInboundEmail(message, env);
	},
} satisfies ExportedHandler<Env>;
