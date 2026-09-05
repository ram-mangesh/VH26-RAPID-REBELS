#!/usr/bin/env python3
"""Run both the Kafka consumer pipeline and the web server"""
import asyncio
import sys
import os

# Add current directory to path
sys.path.insert(0, '/app')

from kafka_consumer import RealEventPipeline
from server import create_app
from aiohttp import web


async def run_both():
    # Create pipeline
    pipeline = RealEventPipeline(num_workers=8)
    
    # Create web app
    app = create_app()
    
    # Override the global PIPELINE in server module
    import server
    server.PIPELINE = pipeline
    
    # Start pipeline
    await pipeline.start()
    
    # Run web server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8081)
    await site.start()
    
    print("Both pipeline and web server started on port 8081")
    
    try:
        # Keep running
        while True:
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        await pipeline.stop()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(run_both())