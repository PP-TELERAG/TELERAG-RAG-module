from config import Configuration
from typing import List, Optional
from chromadb_client import ChromaDBClient
from broker import BrokerGateAway
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
    def __init__(self, config: Configuration = None):
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

        self.broker_client = BrokerGateAway(
            broker_url=config.BROKER_URL,
            broker_doc_topic=config.BROKER_DOC_TOPIC,
            broker_search_in_topic=config.BROKER_SEARCH_IN_TOPIC,
            broker_search_out_topic=config.BROKER_SEARCH_OUT_TOPIC
        )

        self.chroma_search_responses_queue = self.chroma_client.search_responses_queue
        self.llm_response_queue = self.llm_client.llm_response_queue
        self.running = True


    async def get_doc_by_notification(self):
        response = await self.broker_client.consume_doc()
        await self.chroma_client.put_to_document_queue(response)

    async def get_search_query_by_notification(self):
        response = await self.broker_client.consume_search()
        await self.chroma_client.put_to_search_queue(response)


    async def runloop(self):
        while self.running:
            if not self.chroma_search_responses_queue.empty():
                response: ChromaDBSearchResponse = await self.chroma_search_responses_queue.get()
                try:
                    llm_request = LlmRequest(user_id=response.user_id, query=response.query, context=response.response)
                    await self.llm_client.put_request(llm_request)
                    logger.info("Processed ChromDB search request. Put to llm queue...")
                except Exception as e:
                    logger.exception("Exception while routing ChromaDB search request to LLM request queue: {}", e)
                finally:
                    self.chroma_search_responses_queue.task_done()

            if not self.llm_response_queue.empty():
                response: LlmResponse = await self.llm_response_queue.get()
                try:
                    await self.broker_client.produce_llm_response(response)
                    logger.info("Processed LLM response request. Sent to broker...")
                except Exception as e:
                    logger.exception("Exception while routing LLM response request to broker.")
                finally:
                    self.llm_response_queue.task_done()

    async def close(self):
        self.running = False
        await self.chroma_client.close()
        await self.llm_client.close()
        logger.info("Service closed.")






