"""Local runs only: load DART/KRX keys from the Claude desktop korea-stock-mcp config into env (never printed)."""
import json, os, glob
def load():
    if os.environ.get("DART_API_KEY") and os.environ.get("KRX_API_KEY"):
        return
    for f in glob.glob(os.path.expandvars(r"%LOCALAPPDATA%\Claude-Data\*\claude_desktop_config.json")):
        env = json.load(open(f, encoding="utf-8")).get("mcpServers", {}).get("korea-stock-mcp", {}).get("env", {})
        for k in ("DART_API_KEY", "KRX_API_KEY"):
            if env.get(k) and not os.environ.get(k):
                os.environ[k] = env[k]
