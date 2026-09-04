import asyncio
import json
import logging
import os

from aiohttp import web

from pipeline import DataPipeline
from ab_compare import run_ab

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("server")

PIPELINE: DataPipeline | None = None
WS_CLIENTS: set[web.WebSocketResponse] = set()
AB_RESULT: dict | None = None
AB_TASK: asyncio.Task | None = None


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    WS_CLIENTS.add(ws)
    logger.info(f"Dashboard connected ({len(WS_CLIENTS)} clients)")

    try:
        async for msg in ws:
            pass
    finally:
        WS_CLIENTS.discard(ws)
        logger.info(f"Dashboard disconnected ({len(WS_CLIENTS)} clients)")

    return ws


async def broadcast_state():
    while True:
        if WS_CLIENTS and PIPELINE:
            state = await PIPELINE.get_full_state()
            payload = json.dumps(state)
            for ws in list(WS_CLIENTS):
                try:
                    await ws.send_str(payload)
                except Exception:
                    WS_CLIENTS.discard(ws)
        await asyncio.sleep(0.5)


async def handle_get_state(request):
    if not PIPELINE:
        return web.json_response({"error": "pipeline not running"}, status=503)
    state = await PIPELINE.get_full_state()
    return web.json_response(state)


async def handle_set_rate(request):
    data = await request.json()
    rate = data.get("rate", 1000)
    if PIPELINE:
        await PIPELINE.set_rate(rate)
    return web.json_response({"ok": True, "rate": rate})


async def handle_start(request):
    if PIPELINE and not PIPELINE.running:
        await PIPELINE.start()
    return web.json_response({"ok": True, "state": "running"})


async def handle_stop(request):
    if PIPELINE and PIPELINE.running:
        await PIPELINE.stop()
    return web.json_response({"ok": True, "state": "stopped"})


async def handle_reset(request):
    if PIPELINE:
        await PIPELINE.stop()
        await PIPELINE.metrics.reset()
        await PIPELINE.start()
    return web.json_response({"ok": True})


async def handle_demo(request):
    if PIPELINE:
        await PIPELINE.start_auto_demo()
    return web.json_response({"ok": True, "demo": True})


async def handle_demo_stop(request):
    if PIPELINE:
        await PIPELINE.stop_auto_demo()
    return web.json_response({"ok": True, "demo": False})


async def handle_kill_worker(request):
    if not PIPELINE:
        return web.json_response({"error": "pipeline not running"}, status=503)
    res = PIPELINE.kill_worker()
    return web.json_response({"ok": True, **res})


async def handle_fault_enable(request):
    if not PIPELINE:
        return web.json_response({"error": "pipeline not running"}, status=503)
    data = await request.json()
    PIPELINE.enable_faults(float(data.get("failure_rate", 0.5)), int(data.get("max_retries", 2)))
    return web.json_response({"ok": True})


async def handle_fault_disable(request):
    if not PIPELINE:
        return web.json_response({"error": "pipeline not running"}, status=503)
    PIPELINE.disable_faults()
    return web.json_response({"ok": True})


async def handle_ab_compare(request):
    global AB_RESULT, AB_TASK
    if AB_TASK and not AB_TASK.done():
        return web.json_response({"ok": True, "running": True, "done": False})
    data = await request.json()
    rate = data.get("rate", 40000)

    async def _run():
        global AB_RESULT
        AB_RESULT = await run_ab(rate=rate, warmup=1.0, measure=10.0)

    AB_RESULT = None
    AB_TASK = asyncio.create_task(_run())
    return web.json_response({"ok": True, "running": True, "done": False})


async def handle_ab_result(request):
    return web.json_response({"ok": True, "done": bool(AB_RESULT), "result": AB_RESULT})


async def on_startup(app):
    global PIPELINE
    PIPELINE = DataPipeline(num_workers=8)
    await PIPELINE.start()
    app["broadcast_task"] = asyncio.create_task(broadcast_state())
    logger.info("Pipeline started on startup")


async def on_cleanup(app):
    global PIPELINE
    if PIPELINE:
        await PIPELINE.stop()
    app["broadcast_task"].cancel()
    try:
        await app["broadcast_task"]
    except asyncio.CancelledError:
        pass


def create_app():
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/api/state", handle_get_state)
    app.router.add_post("/api/rate", handle_set_rate)
    app.router.add_post("/api/start", handle_start)
    app.router.add_post("/api/stop", handle_stop)
    app.router.add_post("/api/reset", handle_reset)
    app.router.add_post("/api/demo", handle_demo)
    app.router.add_post("/api/demo/stop", handle_demo_stop)
    app.router.add_post("/api/kill-worker", handle_kill_worker)
    app.router.add_post("/api/fault/enable", handle_fault_enable)
    app.router.add_post("/api/fault/disable", handle_fault_disable)
    app.router.add_post("/api/ab", handle_ab_compare)
    app.router.add_get("/api/ab/result", handle_ab_result)

    static_dir = os.path.join(os.path.dirname(__file__), "dashboard")

    async def index(request):
        return web.FileResponse(os.path.join(static_dir, "index.html"))

    app.router.add_get("/", index)
    app.router.add_static("/static", static_dir, name="dashboard")

    return app


if __name__ == "__main__":
    import sys
    port = 8080
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=port)
