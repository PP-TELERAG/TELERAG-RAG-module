import asyncio

import uvicorn
from fastapi import FastAPI
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

    chroma_runloop = asyncio.create_task(service.chroma_client.runloop)
    llm_runloop = asyncio.create_task(service.llm_client.runloop)
    service_runloop = asyncio.create_task(service.runloop)
    api_task = asyncio.create_task(setup_fastapi)

    logger.info("All tasks assigned!")
    await idle()

    chroma_runloop.cancel()
    try:
        await chroma_runloop
    except asyncio.CancelledError:
        pass

    logger.info("ChromaDB client task closed...")

    llm_runloop.cancel()
    try:
        await llm_runloop
    except asyncio.CancelledError:
        pass

    logger.info("LLM client task closed...")

    service_runloop.cancel()
    try:
        await service_runloop
    except asyncio.CancelledError:
        pass

    logger.info("Service client task closed...")

    api_task.cancel()
    try:
        await api_task
    except asyncio.CancelledError:
        pass

    logger.info("API client task closed...")

    await service.close()
    logger.info("Finally closed everything! Can't wait to be rebooted!")



async def idle():
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    logger.info("Service started. To terminate execution press CTRL+C...")
    await stop_event.wait()
    logger.info("Initiated termination process...")



@app.get("/notify/add_doc")
async def notify_add_doc():
    service_instance = Service()
    await service_instance.get_doc_by_notification()

@app.get("/notify/get_search")
async def notify_get_search():
    service_instance = Service()
    await service_instance.get_search_query_by_notification()



async def setup_fastapi():
    uvicorn_config = uvicorn.Config(app=app, host=config.SERVER_HOST, port=config.SERVER_PORT)
    uvicorn_server = uvicorn.Server(uvicorn_config)
    await uvicorn_server.serve()

if __name__ == "__main__":
    asyncio.run(main())