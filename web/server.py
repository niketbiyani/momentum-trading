"""
Web Server — FastAPI + WebSocket broadcast dashboard.

Architecture:
  - FastAPI serves index.html + static files
  - /ws WebSocket endpoint: clients connect and receive live state pushes
  - _broadcast_task() runs every 500ms and pushes current state to all clients
  - update_state() is called from the processor thread (thread-safe via GIL)
  - start_server() launches uvicorn in a daemon thread

Usage (from main.py):
    from web.server import start_server, update_state
    start_server(host="0.0.0.0", port=8000)
    # ... in processing loop:
    update_state(build_state_dict())
"""
import asyncio
import json
import logging
import threading
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger(__name__)

# ── Shared state (updated by processor thread, read by broadcast task) ─────────
# Simple dict replacement is atomic under CPython's GIL — safe for our use case.
_state: dict[str, Any] = {
    "status": "Starting…",
    "timestamp": "--:--:--",
    "active_tf": "1m",
    "stocks": {},
    "signals": [],
}

# Set of active WebSocket connections (accessed only from the asyncio event loop)
_clients: set[WebSocket] = set()

# Reference to the server's event loop (set on startup)
_loop: asyncio.AbstractEventLoop | None = None

# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(title="Options Spike Detector")

_STATIC_DIR = Path(__file__).parent / "static"


@app.on_event("startup")
async def _on_startup() -> None:
    global _loop
    _loop = asyncio.get_event_loop()
    asyncio.create_task(_broadcast_task())
    logger.info("Web server started — broadcast task running")


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse((_STATIC_DIR / "index.html").read_text())


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    _clients.add(ws)
    logger.info(f"WebSocket client connected (total={len(_clients)})")
    try:
        # Send current state immediately on connect
        await ws.send_text(json.dumps(_state))
        # Handle incoming messages (timeframe switch: {"set_tf": "1m"})
        async for raw in ws.iter_text():
            try:
                msg = json.loads(raw)
                if "set_tf" in msg:
                    _state["active_tf"] = msg["set_tf"]
            except Exception:
                pass
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"WebSocket error: {e}")
    finally:
        _clients.discard(ws)
        logger.info(f"WebSocket client disconnected (total={len(_clients)})")


app.mount(
    "/static",
    StaticFiles(directory=str(_STATIC_DIR)),
    name="static",
)


# ── Broadcast task ─────────────────────────────────────────────────────────────

async def _broadcast_task() -> None:
    """Push current state to all connected clients every 500 ms."""
    while True:
        await asyncio.sleep(0.5)
        if not _clients:
            continue
        payload = json.dumps(_state)
        dead: set[WebSocket] = set()
        for ws in list(_clients):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.add(ws)
        _clients -= dead


# ── Public API ─────────────────────────────────────────────────────────────────

def update_state(new_state: dict[str, Any]) -> None:
    """
    Replace the shared state dict from any thread.
    Thread-safe under CPython's GIL for simple dict assignment.
    """
    global _state
    _state = new_state


def get_active_tf() -> str:
    """Return the timeframe currently selected by the browser client."""
    return _state.get("active_tf", "1m")


def start_server(host: str = "0.0.0.0", port: int = 8000) -> threading.Thread:
    """
    Start the uvicorn web server in a daemon thread.
    Returns the thread so the caller can join if needed.
    """
    import uvicorn

    def _run() -> None:
        uvicorn.run(
            app,
            host=host,
            port=port,
            log_level="error",
            access_log=False,
        )

    t = threading.Thread(target=_run, name="web-server", daemon=True)
    t.start()
    logger.info(f"Web server thread started on http://{host}:{port}")
    return t
