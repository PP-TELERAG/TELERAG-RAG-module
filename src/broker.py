import httpx
from models import ChromaDBAddDocumentRequest, ChromaDBSearchQuery, LlmResponse
from loguru import logger
class BrokerGateway:
    def __init__(self, broker_url: str, broker_in_topic: str, broker_out_topic: str):
        self.__broker_consume_url = f"{broker_url}/topics/{broker_in_topic}/consume"
        self.__broker_produce_url = f"{broker_url}/topics/{broker_out_topic}/produce"

    async def consume_message(self) -> ChromaDBSearchQuery or ChromaDBAddDocumentRequest:
        async with httpx.AsyncClient() as client:
            try:
                message = await client.get(self.__broker_consume_url)
                if message.status_code != 200:
                    logger.error("Error consuming message: {}", message.status_code)
                data = message.json()
                message = data.get("Message", {})
                subject = message.get("subject", "").lower()
                body = message.get("body", {})

                if subject == "addDoc":
                    channel_id = body.get("channelId")
                    message_payload = body.get("payload", {})
                    texts = message_payload.get("texts", "")
                    request_obj = ChromaDBAddDocumentRequest(
                        channel_id=int(channel_id),
                        text=texts,
                    )
                elif subject == "search":
                    user_id = body.get("userId")
                    message_payload = body.get("payload", {})
                    query = message_payload.get("query", "")
                    request_obj = ChromaDBSearchQuery(
                        user_id=int(user_id),
                        query=query,
                        top_k=-1,
                        channel_ids= list()
                    )
                else:
                    logger.warning("Unknown subject: {}. Skipping...", subject)

                if request_obj:
                    logger.info("Parsed cunsumed message and transformed it into request object: {}", type(request_obj))
                    return request_obj
            except Exception as e:
                logger.exception("Error consuming message: {}", e)

    async def produce_message(self, message: LlmResponse):
        data = {
            "Message": {
                "method": "",
                "subject": "llmResponse",
                "body": {
                    "userId": message.user_id,
                    "payload": {
                        "response": message.response,
                    }
                }
            }
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(self.__broker_produce_url, json=data)
                if response.status_code != 200:
                    logger.error("Error producing message: {}", response.status_code)
            except Exception as e:
                logger.exception("Exception producing message: {}", e)