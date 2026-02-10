"use client";

import { useEffect, useMemo, useState } from "react";

type ChatMsg = {
  role: "user" | "assistant" | "system";
  content: string;
  ts: number;
};

type FeedEvent = {
  id?: string;
  ts: number;
  agent: string;
  kind: string;
  summary: string;
};

type Props = {
  agent: string;
  title?: string;
  context?: Record<string, unknown>;
  className?: string;
};

function fmtTs(ts: number) {
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString();
}

export default function AiAgentChatPanel({
  agent,
  title = "AI Copilot Chat",
  context = {},
  className = "",
}: Props) {
  const [loading, setLoading] = useState(false);
  const [sending, setSending] = useState(false);
  const [err, setErr] = useState("");
  const [messages, setMessages] = useState<ChatMsg[]>([]);
  const [events, setEvents] = useState<FeedEvent[]>([]);
  const [input, setInput] = useState("");

  async function loadHistory() {
    setLoading(true);
    setErr("");
    try {
      const res = await fetch(
        `/api/ai/chat/history?agent=${encodeURIComponent(agent)}`,
        { cache: "no-store" },
      );
      const json = await res.json();
      if (!res.ok || !json?.ok) {
        throw new Error(json?.error || `HTTP ${res.status}`);
      }
      setMessages(Array.isArray(json.history) ? json.history : []);
      setEvents(Array.isArray(json.events) ? json.events : []);
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : "Failed to load chat history");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadHistory();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [agent]);

  async function send() {
    const text = input.trim();
    if (!text || sending) return;

    setSending(true);
    setErr("");
    setInput("");
    try {
      const res = await fetch("/api/ai/chat", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          agent,
          message: text,
          context,
        }),
      });
      const json = await res.json();
      if (!res.ok || !json?.ok) {
        throw new Error(json?.error || `HTTP ${res.status}`);
      }
      setMessages(Array.isArray(json.history) ? json.history : []);
      const hRes = await fetch(
        `/api/ai/chat/history?agent=${encodeURIComponent(agent)}`,
        { cache: "no-store" },
      );
      const hJson = await hRes.json();
      setEvents(Array.isArray(hJson?.events) ? hJson.events : []);
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : "Failed to send message");
    } finally {
      setSending(false);
    }
  }

  const recentEvents = useMemo(() => {
    return (events || [])
      .slice()
      .reverse()
      .filter((e) => e.agent !== agent || e.kind === "insight_run")
      .slice(0, 8);
  }, [events, agent]);

  return (
    <div className={`aiChatCard ${className}`}>
      <div className="aiChatTop">
        <div>
          <div className="aiTitle">{title}</div>
          <div className="mini" style={{ opacity: 0.85 }}>
            Agent: <b>{agent}</b> · Shared memory enabled across all dashboards.
          </div>
        </div>
        <button
          className="smallBtn"
          type="button"
          onClick={loadHistory}
          disabled={loading || sending}
        >
          {loading ? "Loading..." : "Refresh"}
        </button>
      </div>

      <div className="aiChatBody">
        <div className="aiChatMessages">
          {messages.length ? (
            messages.map((m, i) => (
              <div
                key={`${m.ts}_${i}`}
                className={`aiMsg ${m.role === "user" ? "aiMsgUser" : "aiMsgAssistant"}`}
              >
                <div className="aiMsgMeta">
                  <span>{m.role === "user" ? "You" : "AI"}</span>
                  <span>{fmtTs(m.ts)}</span>
                </div>
                <div className="aiMsgText">{m.content}</div>
              </div>
            ))
          ) : (
            <div className="mini">No messages yet.</div>
          )}
        </div>

        <aside className="aiFeed">
          <div className="mini" style={{ opacity: 0.8, marginBottom: 8 }}>
            Recent AI feed (all agents)
          </div>
          {recentEvents.length ? (
            <div className="aiFeedList">
              {recentEvents.map((e, i) => (
                <div className="aiFeedItem" key={`${e.ts}_${i}`}>
                  <div className="aiFeedMeta">
                    <span>{e.agent}</span>
                    <span>{fmtTs(e.ts)}</span>
                  </div>
                  <div className="aiFeedText">{e.summary}</div>
                </div>
              ))}
            </div>
          ) : (
            <div className="mini">No feed events yet.</div>
          )}
        </aside>
      </div>

      <div className="aiChatComposer">
        <textarea
          className="input aiChatInput"
          placeholder="Ask the AI agent about business issues, root causes, and action plans..."
          value={input}
          onChange={(e) => setInput(e.target.value)}
          disabled={sending}
          rows={3}
        />
        <button className="btn btnPrimary" type="button" onClick={send} disabled={sending || !input.trim()}>
          {sending ? "Sending..." : "Send"}
        </button>
      </div>

      {err ? (
        <div className="mini" style={{ color: "var(--danger)", marginTop: 8 }}>
          X {err}
        </div>
      ) : null}
    </div>
  );
}
