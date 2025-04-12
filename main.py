import asyncio

import uvicorn
from fastapi import FastAPI, responses
from loguru import logger
from src import get_config
from src import Service
import signal

app = FastAPI()
config = get_config()
logger.add("logs/latest-RAG.log", level=config.LOG_LEVEL, rotation=config.LOGGER_ROTATION, retention=config.LOGGER_RETENTION, encoding=config.LOGGER_ENCODING)


async def main():
    logger.info("Starting RAG-module. Wait for a couple of seconds...")
    service = Service(config=config)
    api_task = await service.start_routines()
    await asyncio.create_task(setup_fastapi)

    await idle()

    await service.stop_routines()
    logger.info("RAG-module stopped.")

    api_task.cancel()
    try:
        await api_task
    except asyncio.CancelledError:
        logger.info("FastAPI stopped.")





async def idle():
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    logger.info("Service started. To terminate execution press CTRL+C...")
    await stop_event.wait()
    logger.info("Initiated termination process...")



@app.get("/notify")
async def notify():
    service = Service.get_instance()
    await service.consume_by_notification()
    return responses.Response(status_code=200)

async def setup_fastapi():
    uvicorn_config = uvicorn.Config(app=app, host=config.SERVER_HOST, port=config.SERVER_PORT)
    uvicorn_server = uvicorn.Server(uvicorn_config)
    await uvicorn_server.serve()

if __name__ == "__main__":
    asyncio.run(main())