"""
Slack slash command handlers for THREAD.

Commands (register these in api.slack.com → Slash Commands):
  /thread-fail       Trigger a simulated payment failure end-to-end
  /thread-recover    Replay the last failed transaction
  /thread-status     Show live system health via MCP
  /thread-search     Plain-English search — Groq generates SPL, executed via MCP
  /thread-analyse    Transaction timeline by correlationId

All commands ack() immediately then do work asynchronously via respond().
"""

import os
import uuid

import httpx

ORDER_SERVICE_URL   = os.getenv("ORDER_SERVICE_URL",   "http://order-service:8001")
PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL", "http://payment-service:8002")
GROQ_API_KEY        = os.getenv("GROQ_API_KEY", "")
SPLUNK_INDEX        = os.getenv("SPLUNK_INDEX", "thread_logs")

_last_failed_correlation_id: str = ""


def set_last_failed(correlation_id: str) -> None:
    global _last_failed_correlation_id
    _last_failed_correlation_id = correlation_id


def _fmt_table(rows: list[dict], cols: list[tuple[str, int]]) -> str:
    """Format rows as a fixed-width monospace table for Slack code blocks.

    cols: [(field_name, max_width), ...]
    """
    header = "  ".join(name.ljust(w) for name, w in cols)
    sep    = "  ".join("-" * w for _, w in cols)
    lines  = [header, sep]
    for row in rows:
        cells = []
        for name, w in cols:
            val = str(row.get(name, "")).replace("\n", " ")
            cells.append(val[:w].ljust(w))
        lines.append("  ".join(cells))
    return "```\n" + "\n".join(lines) + "\n```"


async def _groq_to_spl(user_query: str) -> str:
    """Call Groq to turn plain English into a Splunk SPL query string."""
    prompt = f"""You are a Splunk SPL expert. Convert the user's plain-English request into a single SPL query.

Index: {SPLUNK_INDEX}
Fields: correlationId, transactionId, sourceService, targetService, traceEvent, statusCode, durationMs, errorMessage, replayAttempt
traceEvent values: REQUEST_START, REQUEST_END, REQUEST_ERROR
Services: order-service, payment-service, inventory-service
Time range: apply earliest=-1h unless the user specifies otherwise.

Rules:
- Return ONLY the raw SPL string, no markdown, no explanation, no backticks.
- Do NOT include a leading "search" keyword.
- Always end with a | table command showing the most useful fields.
- Use camelCase field names exactly as listed above.

Examples:
User: show all payment failures
SPL: index={SPLUNK_INDEX} targetService="payment-service" traceEvent=REQUEST_ERROR earliest=-1h | table _time, correlationId, sourceService, errorMessage, statusCode, durationMs

User: slow requests over 1 second
SPL: index={SPLUNK_INDEX} traceEvent=REQUEST_END earliest=-1h | where durationMs>1000 | sort by durationMs desc | table _time, correlationId, sourceService, targetService, durationMs, statusCode

User: {user_query}
SPL:"""

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 256,
            },
        )
        resp.raise_for_status()
        spl = resp.json()["choices"][0]["message"]["content"].strip()
        # Strip any accidental backtick fences
        if spl.startswith("```"):
            spl = spl.strip("`").strip()
        return spl


def register_commands(app) -> None:
    """Register all slash command handlers on the Bolt AsyncApp."""

    @app.command("/thread-fail")
    async def cmd_fail(ack, respond, command):
        """Enable payment failure then place an order — triggers the full THREAD pipeline."""
        await ack()
        await respond("⚡ Triggering payment failure simulation...")

        async with httpx.AsyncClient(timeout=5) as c:
            try:
                toggle = await c.post(f"{PAYMENT_SERVICE_URL}/admin/toggle-failure")
                state = toggle.json()
                if not state.get("simulate_failure"):
                    await c.post(f"{PAYMENT_SERVICE_URL}/admin/toggle-failure")
            except Exception as e:
                await respond(f"❌ Could not reach payment-service: {e}")
                return

        await respond("💥 Failure mode *ON* — placing order now...")

        try:
            async with httpx.AsyncClient(timeout=10) as c:
                resp = await c.post(
                    f"{ORDER_SERVICE_URL}/api/v1/orders",
                    json={
                        "customer_id": f"demo-{uuid.uuid4().hex[:6]}",
                        "items": [{"productId": "prod-demo", "quantity": 1}],
                        "total": 99.99,
                    },
                )
                data = resp.json()
                corr_id = data.get("correlation_id", "")
                if corr_id:
                    set_last_failed(corr_id)
                    await respond({
                        "response_type": "in_channel",
                        "text": (
                            f"🔴 Order failed as expected.\n"
                            f"Correlation ID: `{corr_id}`\n"
                            f"THREAD is investigating — check #thread-alerts for the alert and Replay button."
                        ),
                    })
                else:
                    await respond(f"Order response: {data}")
        except Exception as e:
            await respond(f"Order placed (failure expected): {e}")

    @app.command("/thread-recover")
    async def cmd_recover(ack, respond, command):
        """Disable failure mode — subsequent orders will succeed (use after /thread-fail demo)."""
        await ack()
        await respond("🔧 Disabling failure simulation...")

        async with httpx.AsyncClient(timeout=5) as c:
            try:
                toggle = await c.post(f"{PAYMENT_SERVICE_URL}/admin/toggle-failure")
                state = toggle.json()
                if state.get("simulate_failure"):
                    await c.post(f"{PAYMENT_SERVICE_URL}/admin/toggle-failure")
            except Exception as e:
                await respond(f"❌ Could not reach payment-service: {e}")
                return

        await respond({
            "response_type": "in_channel",
            "text": (
                "✅ Failure mode *OFF* — payment-service is healthy.\n"
                "Click *Replay* on the Slack alert above to re-execute the failed transaction."
            ),
        })

    @app.command("/thread-status")
    async def cmd_status(ack, respond, command):
        """Show live system health for all services via Splunk MCP."""
        await ack()
        await respond("🔍 Querying Splunk MCP for system health...")

        from ..splunk.mcp_client import SplunkMCPClient

        services = ["order-service", "payment-service", "inventory-service"]
        lines = ["*📊 Service Health Status (last 15m)*\n"]

        async with SplunkMCPClient() as mcp:
            for svc in services:
                health = await mcp.get_service_health(svc, window="-15m")
                total = health.get("total", 0)
                if total == 0:
                    lines.append(f"• `{svc}` — no data")
                else:
                    rate = health.get("error_rate", 0)
                    ok   = health.get("health_pct", 100)
                    icon = "🟢" if rate == 0 else ("🟡" if rate < 20 else "🔴")
                    lines.append(f"• {icon} `{svc}` — {ok:.0f}% healthy, {rate:.1f}% errors ({total} reqs)")

            errors = await mcp.get_system_errors(window="-15m")
        lines.append(f"\n*Recent failures:* {len(errors)} transaction(s) affected")

        await respond({"response_type": "in_channel", "text": "\n".join(lines)})

    @app.command("/thread-search")
    async def cmd_search(ack, respond, command):
        """Plain-English Splunk search — Groq generates the SPL, executed via MCP."""
        await ack()
        query_text = (command.get("text") or "").strip()

        if not query_text:
            await respond("Usage: `/thread-search <plain English>` e.g. `/thread-search show payment failures last hour`")
            return

        await respond(f"🤖 Generating SPL via Splunk AI for: _{query_text}_...")

        from ..splunk.mcp_client import SplunkMCPClient

        async with SplunkMCPClient() as mcp:
            spl = await mcp.generate_spl(query_text)
            if not spl:
                await respond(f"❌ Splunk AI could not generate SPL for: `{query_text}`")
                return

            print(f"[THREAD:SEARCH] Splunk AI generated SPL: {spl}")
            results = await mcp.search(spl, earliest="-1h", _label=f"nl_search({query_text[:40]})")

        if not results:
            await respond(
                f"*🔎 No results for:* _{query_text}_\n"
                f"Generated SPL: `{spl[:200]}`"
            )
            return

        # Detect columns from first row, prioritise known useful fields
        priority = ["_time", "correlationId", "sourceService", "targetService", "traceEvent", "statusCode", "durationMs", "errorMessage"]
        first = results[0]
        cols_present = [f for f in priority if f in first]
        # Add any extra fields the LLM selected that aren't in priority list
        for f in first:
            if f not in cols_present and not f.startswith("_"):
                cols_present.append(f)

        col_widths = {
            "_time": 19, "correlationId": 36, "sourceService": 17, "targetService": 17,
            "traceEvent": 13, "statusCode": 6, "durationMs": 10, "errorMessage": 40,
        }
        cols = [(f, col_widths.get(f, 20)) for f in cols_present]

        table = _fmt_table(results[:15], cols)
        header = f"*🔎 {len(results)} result(s) for:* _{query_text}_\nSPL: `{spl[:200]}`\n"
        suffix = f"\n_Showing {min(15, len(results))} of {len(results)} rows_" if len(results) > 15 else ""

        await respond({"response_type": "in_channel", "text": header + table + suffix})

    @app.command("/thread-analyse")
    async def cmd_analyse(ack, respond, command):
        """Show full transaction timeline for a correlationId via Splunk MCP."""
        await ack()
        correlation_id = (command.get("text") or "").strip()

        if not correlation_id:
            await respond("Usage: `/thread-analyse <correlationId>`")
            return

        await respond(f"🔍 Fetching transaction chain for `{correlation_id}`...")

        from ..splunk.mcp_client import SplunkMCPClient

        async with SplunkMCPClient() as mcp:
            results = await mcp.get_transaction_chain(correlation_id)

        if not results:
            await respond(f"No events found for correlation ID `{correlation_id}`.")
            return

        cols = [
            ("_time",         19),
            ("traceEvent",    13),
            ("sourceService", 17),
            ("targetService", 17),
            ("statusCode",     6),
            ("durationMs",    10),
            ("errorMessage",  40),
        ]

        table = _fmt_table(results, cols)
        n = len(results)
        errors = sum(1 for r in results if r.get("traceEvent") == "REQUEST_ERROR")
        icon = "❌" if errors else "✅"

        header = f"*{icon} Transaction `{correlation_id}`* · {n} events"
        if errors:
            header += f" · {errors} error(s)"
        header += "\n"

        await respond({"response_type": "in_channel", "text": header + table})
