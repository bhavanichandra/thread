"""
Dashboard generation — uses Groq LLM to create 3 SPL queries, then creates
a dashboard in Splunk via REST API.
"""
import os
import json
import httpx
import logging

logger = logging.getLogger("thread-platform")

SPLUNK_HOST = os.getenv("SPLUNK_HOST", "localhost")
SPLUNK_PORT = int(os.getenv("SPLUNK_PORT", "8089"))
SPLUNK_USERNAME = os.getenv("SPLUNK_USERNAME", "admin")
SPLUNK_PASSWORD = os.getenv("SPLUNK_PASSWORD", "")
SPLUNK_BASE_URL = os.getenv("SPLUNK_BASE_URL", "http://localhost:8000")
SPLUNK_INDEX = os.getenv("SPLUNK_INDEX", "thread_logs")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")


async def generate_dashboard(
    correlation_id: str,
    failed_service: str,
    error_message: str,
    error_rate_pct: float,
    anomaly_score: float,
) -> str:
    """
    Generate a Splunk dashboard with 3 AI-created SPL panels.
    Returns the dashboard URL, or empty string on failure.
    """
    if not GROQ_API_KEY:
        logger.warning("[THREAD:DASHBOARD] GROQ_API_KEY not set, skipping dashboard generation")
        return ""

    short_id = correlation_id[:8]

    # ── Call Groq to generate 3 SPL queries ───────────────────────────────────
    prompt = f"""Generate 3 Splunk dashboard panels for a transaction failure investigation.
Respond ONLY with a JSON array, no markdown, no explanation.

Context:
- Failed service: {failed_service}
- Error: {error_message}
- Correlation ID: {correlation_id}
- Error rate: {error_rate_pct:.1f}%
- Anomaly score: {anomaly_score:.2f}
- Time range: last 1 hour
- Index: {SPLUNK_INDEX}

Panel 1: Transaction chain for this failure
Panel 2: Error rate trends for the failed service
Panel 3: System-wide error distribution

Field names to use: correlationId, sourceService, targetService, traceEvent,
statusCode, durationMs, errorMessage, replayAttempt

Format response as a JSON array (no markdown, no backticks):
[
  {{"title": "Panel 1", "spl": "...", "viz": "table"}},
  {{"title": "Panel 2", "spl": "...", "viz": "timechart"}},
  {{"title": "Panel 3", "spl": "...", "viz": "bar"}}
]"""

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "mixtral-8x7b-32768",
                    "messages": [
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.3,
                    "max_tokens": 1024,
                },
            )
            resp.raise_for_status()
            data = resp.json()

            # Extract the content
            content = data["choices"][0]["message"]["content"].strip()

            # Parse the JSON array (Groq might wrap it in markdown code blocks)
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            panels_spec = json.loads(content)
            if not isinstance(panels_spec, list):
                raise ValueError("Response is not an array")

    except Exception as e:
        logger.warning(f"[THREAD:DASHBOARD] LLM generation failed: {e}")
        return ""

    # ── Build Splunk dashboard XML ────────────────────────────────────────────
    dashboard_name = f"thread-{short_id}"
    dashboard_xml = _build_dashboard_xml(dashboard_name, correlation_id, panels_spec)

    # ── Create dashboard in Splunk via REST API ───────────────────────────────
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            resp = await client.post(
                f"https://{SPLUNK_HOST}:{SPLUNK_PORT}/servicesNS/admin/search/data/ui/views",
                auth=(SPLUNK_USERNAME, SPLUNK_PASSWORD),
                data={
                    "name": dashboard_name,
                    "eai:data": dashboard_xml,
                },
            )
            resp.raise_for_status()
            logger.info(f"[THREAD:DASHBOARD] Created dashboard {dashboard_name}")

    except Exception as e:
        logger.warning(f"[THREAD:DASHBOARD] Failed to create dashboard: {e}")
        return ""

    # ── Return dashboard URL ──────────────────────────────────────────────────
    dashboard_url = f"{SPLUNK_BASE_URL}/en-US/app/search/{dashboard_name}"
    print(f"[THREAD:MCP] AI-generated dashboard → {dashboard_url}")
    return dashboard_url


def _build_dashboard_xml(name: str, correlation_id: str, panels_spec: list) -> str:
    """Build the dashboard XML with 3 panels from the LLM spec."""
    panels_xml = ""
    for i, panel in enumerate(panels_spec, 1):
        title = panel.get("title", f"Panel {i}")
        spl = panel.get("spl", "")
        viz = panel.get("viz", "table").lower()

        if viz == "timechart":
            panels_xml += f"""  <row>
    <panel>
      <title>{title}</title>
      <chart>
        <search><query>{spl}</query><earliest>-1h</earliest><latest>now</latest></search>
        <option name="charting.chart">line</option>
      </chart>
    </panel>
  </row>
"""
        elif viz in ("bar", "column"):
            panels_xml += f"""  <row>
    <panel>
      <title>{title}</title>
      <chart>
        <search><query>{spl}</query><earliest>-1h</earliest><latest>now</latest></search>
        <option name="charting.chart">bar</option>
      </chart>
    </panel>
  </row>
"""
        else:  # table, default
            panels_xml += f"""  <row>
    <panel>
      <title>{title}</title>
      <table>
        <search><query>{spl}</query><earliest>-1h</earliest><latest>now</latest></search>
      </table>
    </panel>
  </row>
"""

    return f"""<dashboard version="1.1">
  <label>THREAD - {correlation_id[:8]}</label>
  <description>Auto-generated by THREAD AI Agent</description>
{panels_xml}</dashboard>"""
