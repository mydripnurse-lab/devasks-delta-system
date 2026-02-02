// src/app/api/stream/[runId]/route.ts
import { getRun } from "@/lib/runStore";

export const runtime = "nodejs";

function sseLine(str: string) {
    return `${str}\n`;
}

function sseEvent(event: string, data: any) {
    return (
        sseLine(`event: ${event}`) +
        sseLine(`data: ${typeof data === "string" ? data : JSON.stringify(data)}`) +
        sseLine("")
    );
}

function parseProgressLine(line: string): { kind: "progress"; json: string } | null {
    const s = String(line ?? "");

    const prefixes = ["__PROGRESS_INIT__ ", "__PROGRESS__ ", "__PROGRESS_END__ "];
    for (const p of prefixes) {
        if (s.startsWith(p)) {
            const json = s.slice(p.length);
            try {
                JSON.parse(json);
                return { kind: "progress", json };
            } catch {
                return null;
            }
        }
    }
    return null;
}

export async function GET(_req: Request, ctx: { params: Promise<{ runId: string }> }) {
    const { runId } = await ctx.params;

    if (!runId) return new Response("Missing runId", { status: 400 });

    const stream = new ReadableStream({
        start(controller) {
            const enc = new TextEncoder();
            const write = (chunk: string) => controller.enqueue(enc.encode(chunk));

            // ✅ SSE reconnect suggestion
            write(sseLine("retry: 1500"));
            write(sseLine(""));

            // hello
            write(sseEvent("hello", { runId }));

            let lastSentLineCount = 0;

            const tick = () => {
                const run = getRun(runId);

                if (!run) {
                    write(sseEvent("end", { runId, ok: false, reason: "not_found" }));
                    controller.close();
                    return;
                }

                // send new lines
                const lines = run.lines || [];
                for (let i = lastSentLineCount; i < lines.length; i++) {
                    const line = String(lines[i] ?? "");

                    const p = parseProgressLine(line);
                    if (p) {
                        write(sseEvent("progress", p.json));
                    } else {
                        write(sseEvent("line", line));
                    }
                }
                lastSentLineCount = lines.length;

                // heartbeat
                write(sseEvent("ping", { t: Date.now() }));

                // finished
                if (run.finished) {
                    write(
                        sseEvent("end", {
                            runId,
                            ok: !run.error,
                            exitCode: run.exitCode ?? null,
                            error: run.error ?? null,
                        })
                    );
                    controller.close();
                    return;
                }

                setTimeout(tick, 800);
            };

            tick();
        },
    });

    return new Response(stream, {
        headers: {
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-cache, no-transform",
            Connection: "keep-alive",
        },
    });
}
