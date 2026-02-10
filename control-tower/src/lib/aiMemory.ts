import fs from "fs/promises";
import path from "path";

export type AiRole = "user" | "assistant" | "system";

export type AiMessage = {
    role: AiRole;
    content: string;
    ts: number;
};

export type AiEvent = {
    id: string;
    ts: number;
    agent: string;
    kind: "insight_run" | "chat_turn";
    summary: string;
    metadata?: Record<string, unknown>;
};

type MemoryStore = {
    version: 1;
    updatedAt: number;
    conversations: Record<string, AiMessage[]>;
    events: AiEvent[];
};

const MAX_EVENTS = 1500;
const MAX_MESSAGES_PER_AGENT = 200;

function memoryPath() {
    return path.join(process.cwd(), "storage", "ai-memory.json");
}

function nowTs() {
    return Date.now();
}

function uid() {
    return `${nowTs()}_${Math.random().toString(36).slice(2, 10)}`;
}

function defaultStore(): MemoryStore {
    return {
        version: 1,
        updatedAt: nowTs(),
        conversations: {},
        events: [],
    };
}

async function ensureDir() {
    const p = memoryPath();
    await fs.mkdir(path.dirname(p), { recursive: true });
}

export async function readAiMemory(): Promise<MemoryStore> {
    const p = memoryPath();
    try {
        const raw = await fs.readFile(p, "utf8");
        const parsed = JSON.parse(raw) as Partial<MemoryStore>;
        return {
            version: 1,
            updatedAt: Number(parsed.updatedAt || nowTs()),
            conversations: parsed.conversations || {},
            events: Array.isArray(parsed.events) ? parsed.events : [],
        };
    } catch {
        return defaultStore();
    }
}

async function writeAiMemory(store: MemoryStore) {
    await ensureDir();
    store.updatedAt = nowTs();
    await fs.writeFile(memoryPath(), JSON.stringify(store, null, 2), "utf8");
}

export async function appendAiEvent(event: Omit<AiEvent, "id" | "ts">) {
    const store = await readAiMemory();
    const next: AiEvent = {
        id: uid(),
        ts: nowTs(),
        agent: event.agent,
        kind: event.kind,
        summary: event.summary,
        metadata: event.metadata || {},
    };
    store.events.push(next);
    if (store.events.length > MAX_EVENTS) {
        store.events = store.events.slice(store.events.length - MAX_EVENTS);
    }
    await writeAiMemory(store);
    return next;
}

export async function appendConversationMessage(agent: string, msg: Omit<AiMessage, "ts">) {
    const store = await readAiMemory();
    if (!store.conversations[agent]) store.conversations[agent] = [];
    store.conversations[agent].push({
        role: msg.role,
        content: msg.content,
        ts: nowTs(),
    });
    if (store.conversations[agent].length > MAX_MESSAGES_PER_AGENT) {
        store.conversations[agent] = store.conversations[agent].slice(
            store.conversations[agent].length - MAX_MESSAGES_PER_AGENT,
        );
    }
    await writeAiMemory(store);
    return store.conversations[agent];
}

export async function getConversation(agent: string, limit = 60) {
    const store = await readAiMemory();
    const msgs = store.conversations[agent] || [];
    return msgs.slice(Math.max(0, msgs.length - Math.max(1, limit)));
}

export async function getRecentEvents(limit = 120) {
    const store = await readAiMemory();
    const events = store.events || [];
    return events.slice(Math.max(0, events.length - Math.max(1, limit)));
}
