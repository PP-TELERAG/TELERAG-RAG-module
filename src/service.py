from config import Configuration
from typing import List, Optional
from chromadb_client import ChromaDBClient
from broker import BrokerGateway
from models import *
from llm_client import LLMClient
from loguru import logger
import asyncio
class Singleton(type):
    _instance = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instance:
            cls._instance[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instance[cls]

class Service(metaclass=Singleton):
    _instance : Optional["Service"] = None

    @classmethod
    def get_instance(cls) -> "Service":
        return cls._instance

    def __init__(self, config: Configuration):
        self.llm_client = LLMClient(
            api_key=config.LLM_API_KEY,
            api_url=config.LLM_API_URL,
            model_name=config.LLM_MODEL_NAME,
            superprompt=config.LLM_SUPERPROMPT)

        self.chroma_client = ChromaDBClient(
            db_path=config.CHROMA_DB_PATH,
            collection_name=config.CHROMA_DB_COLLECTION_NAME,
            model_name=config.SENTENCE_TRANSFORMERS_MODEL_NAME
        )

        self.broker_client = BrokerGateway(
            broker_url=config.BROKER_URL,
            broker_in_topic=config.BROKER_IN_TOPIC,
            broker_out_topic=config.BROKER_OUT_TOPIC,
        )

        self.entry_queue = asyncio.Queue()
        self.output_queue = asyncio.Queue()
        self._gather_task = None

    async def chroma_task(self):
        if self.entry_queue.empty():
            await asyncio.sleep(0.1)
            return
        try:
            request = await self.entry_queue.get()
            if type(request) is not ChromaDBSearchQuery and type(request) is not ChromaDBAddDocumentRequest:
                logger.warning("Unsupported Chroma DB request type. Expected ChromaDBSearchQuery or ChromaDBAddDocumentRequest got {}", type(request))
                await asyncio.sleep(0.1)
                return

            if type(request) is ChromaDBAddDocumentRequest:
                ids = await self.chroma_client.add_document(request)
                logger.info("Added {}\n ids", ids)
            elif type(request) is ChromaDBSearchQuery:
                chroma_response: ChromaDBSearchResponse = await self.chroma_client.search(request)
                llm_request: LlmRequest = LlmRequest(
                    user_id=chroma_response.user_id,
                    query=chroma_response.query,
                    context=chroma_response.response
                )
                llm_response: LlmResponse = await self.llm_client.query(llm_request)
                await self.output_queue.put(llm_response)
                logger.info("Performed search query.")
        except Exception as e:
            logger.exception("Exception occurred in chroma db task: {}", e)
        finally:
            self.entry_queue.task_done()
            await asyncio.sleep(0.1)

    async def production_task(self):
        if self.output_queue.empty():
            await asyncio.sleep(0.1)
            return
        try:
            request = await self.output_queue.get()
            if type(request) is not LlmResponse:
                logger.warning("Unsupported Llm response type. Expected LlmResponse got {}", type(request))
                await asyncio.sleep(0.1)
                return

            await self.broker_client.produce_message(request)
            logger.info("Send response to broker.")
        except Exception as e:
            logger.exception("Exception occurred in production task: {}", e)
        finally:
            self.output_queue.task_done()
            await asyncio.sleep(0.1)

    async def _chroma_runloop(self):
        while True:
            await self.chroma_task()

    async def _production_runloop(self):
        while True:
            await self.production_task()

    async def consume_by_notification(self):
        request = await self.broker_client.consume_message()
        if type(request) is not ChromaDBSearchQuery and type(request) is not ChromaDBAddDocumentRequest:
            logger.warning("Unsupported request type got while routing request to a queue: {}", type(request))
            return
        await self.entry_queue.put(request)

    async def start_routines(self):
        self._gather_task = asyncio.create_task(
            asyncio.gather(
                self._chroma_runloop(),
                self._production_runloop()
            )
        )
        logger.info("Service routines started.")

    async def stop_routines(self):
        if self._gather_task is not None:
            self._gather_task.cancel()
            try:
                await self._gather_task
            except asyncio.CancelledError:
                logger.info("Service routines cancelled.")
        self._gather_task = None




