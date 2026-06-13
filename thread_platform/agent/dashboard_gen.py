"""
Dashboard generation — uses Groq LLM to create 3 SPL queries, then creates
a dashboard in Splunk via REST API.
"""
import os
import json
import httpx
import logging
from html import escape as xml_escape

logger = logging.getLogger("thread-platform")

SPLUNK_HOST = os.getenv("SPLUNK_HOST", "localhost")
SPLUNK_PORT = int(os.getenv("SPLUNK_PORT", "8089"))
SPLUNK_USERNAME = os.getenv("SPLUNK_USERNAME", "admin")
SPLUNK_PASSWORD = os.getenv("SPLUNK_PASSWORD", "")
SPLUNK_BASE_URL = os.getenv("SPLUNK_BASE_URL", "http://localhost:8000")
SPLUNK_INDEX = os.getenv("SPLUNK_INDEX", "thread_logs")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")


def _validate_panel_specs(specs: list) -> tuple[bool, list]:
    """
    Validate LLM-generated panel specs.

    Returns: (is_valid, validated_specs)
    - Checks: exactly 3 panels, each has non-empty title and spl, viz is recognized
    - Escapes: title and spl to prevent XML injection
    - Returns empty list on validation failure
    """
    if not isinstance(specs, list):
        logger.warning("[THREAD:DASHBOARD] Panel specs is not a list")
        return False, []

    if len(specs) != 3:
        logger.warning(f"[THREAD:DASHBOARD] Expected 3 panels, got {len(specs)}")
        return False, []

    validated = []
    for i, panel in enumerate(specs):
        if not isinstance(panel, dict):
            logger.warning(f"[THREAD:DASHBOARD] Panel {i} is not a dict")
            return False, []

        title = panel.get("title", "").strip()
        spl = panel.get("spl", "").strip()
        viz = panel.get("viz", "table").lower()

        if not title or not spl:
            logger.warning(
                f"[THREAD:DASHBOARD] Panel {i} missing title or spl: "
                f"title='{title}', spl_len={len(spl)}"
            )
            return False, []

        if viz not in ("table", "timechart", "bar", "column"):
            logger.warning(f"[THREAD:DASHBOARD] Panel {i} unknown viz type: {viz}")
            return False, []

        if len(spl) > 1000:
            logger.warning(
                f"[THREAD:DASHBOARD] Panel {i} SPL too long ({len(spl)} chars), truncating to 1000"
            )
            spl = spl[:1000]

        # Escape title to prevent XML injection; sanitize CDATA terminator in SPL
        safe_spl = spl.replace("]]>", "]]]]><![CDATA[>")
        validated.append({
            "title": xml_escape(title)[:100],
            "spl": safe_spl,
            "viz": viz,
        })

    return True, validated


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
                    "model": "llama-3.3-70b-versatile",
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

            # Strip optional markdown code fences; handle any language tag robustly
            if "```" in content:
                parts = content.split("```")
                if len(parts) >= 2:
                    inner = parts[1]
                    lines = inner.split("\n", 1)
                    first = lines[0].strip()
                    # Treat as a language tag only if it's short and has no spaces
                    if first and len(first) <= 20 and " " not in first:
                        inner = lines[1] if len(lines) > 1 else ""
                    content = inner.strip()

            panels_spec = json.loads(content)

    except Exception as e:
        logger.warning(f"[THREAD:DASHBOARD] LLM generation failed: {e}")
        return ""

    # ── Validate panel specs ──────────────────────────────────────────────────
    is_valid, validated_specs = _validate_panel_specs(panels_spec)
    if not is_valid:
        logger.warning("[THREAD:DASHBOARD] Panel validation failed, skipping dashboard")
        return ""

    panels_spec = validated_specs

    # ── Build Splunk dashboard XML ────────────────────────────────────────────
    dashboard_name = f"thread-{short_id}"
    dashboard_xml = _build_dashboard_xml(dashboard_name, correlation_id, panels_spec)

    # ── Create dashboard in Splunk via REST API ───────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
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
    """Build the dashboard XML with 3 panels from the LLM spec.

    Uses CDATA for SPL queries to preserve literal text without XML escaping.
    Uses escaped title to prevent injection into XML attributes.
    """
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
        <search><query><![CDATA[{spl}]]></query><earliest>-1h</earliest><latest>now</latest></search>
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
        <search><query><![CDATA[{spl}]]></query><earliest>-1h</earliest><latest>now</latest></search>
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
        <search><query><![CDATA[{spl}]]></query><earliest>-1h</earliest><latest>now</latest></search>
      </table>
    </panel>
  </row>
"""

    return f"""<dashboard version="1.1">
  <label>THREAD - {correlation_id[:8]}</label>
  <description>Auto-generated by THREAD AI Agent</description>
{panels_xml}</dashboard>"""
