import httpx
from models import ChromaDBAddDocumentRequest, ChromaDBSearchQuery, LlmResponse
from loguru import logger
class BrokerGateAway:
    def __init__(self, broker_url: str, broker_doc_topic: str, broker_search_in_topic: str, broker_search_out_topic: str):
        self.__broker_consume_search_query_url = f"{broker_url}/topics/{broker_search_in_topic}/consume"
        self.__broker_consume_doc_topic_url = f"{broker_url}/topics/{broker_doc_topic}/consume"
        self.__broker_produce_search_query_url = f"{broker_url}/topics/{broker_search_out_topic}/produce"

    async def consume_doc(self) -> ChromaDBAddDocumentRequest:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(self.__broker_consume_doc_topic_url)
                if response.status_code != 200:
                    logger.error("Document consumption response arrived with status code {}", response.status_code)
                    return ChromaDBAddDocumentRequest(text="<NO RESPONSE>")
                jsoned = response.json()
                chroma_request = ChromaDBAddDocumentRequest(channel_id=jsoned["channelId"], document_id=jsoned["documentId"], text=jsoned["text"])
            except Exception as e:
                logger.exception("Exception while consuming document: {}", e)
                return ChromaDBAddDocumentRequest(text="<NO RESPONSE>")
            finally:
                return chroma_request

    async def consume_search(self) -> ChromaDBSearchQuery:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(self.__broker_consume_search_query_url)
                if response.status_code != 200:
                    logger.error("Search query consumption response arrived with status code {}", response.status_code)
                    return ChromaDBSearchQuery(query="<NO RESPONSE>")
                jsoned = response.json()
                chroma_request = ChromaDBSearchQuery(user_id=jsoned["userId"], query=jsoned["query"], channel_ids=jsoned["channelIds"], top_k=jsoned["topK"])
            except Exception as e:
                logger.exception("Exception while consuming search query: {}", e)
                return ChromaDBSearchQuery(query="<NO RESPONSE>")
            finally:
                return chroma_request

    async def produce_llm_response(self, response: LlmResponse):
        response_body = {
            "userId": str(response.userId),
            "response": response.response
        }
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(self.__broker_produce_search_query_url, json=response_body)
                if response.status_code != 200:
                    logger.error("Llm response production returned with status code {}", response.status_code)
                    return
                logger.info("Produced Llm response")
            except Exception as e:
                logger.exception("Exception while producing llm response: {}", e)
                return






