import asyncio
from typing import List
import httpx
from loguru import logger
from models import LlmRequest, LlmResponse


class LLMClient:
    def __init__(self, api_key: str, api_url: str, model_name: str, superprompt: str):
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.url = api_url
        self.MODEL_NAME = model_name
        self.SUPERPROMPT = superprompt

        self.__llm_request_queue = asyncio.Queue()
        self.llm_response_queue = asyncio.Queue()
        self.running = True

    async def put_request(self, request: LlmRequest):
        await self.__llm_request_queue.put(request)

    async def __query(self, request: LlmRequest) -> LlmResponse:
        context = "\n".join(LlmRequest.context)
        query = LlmRequest.query

        data = {
            "model": self.MODEL_NAME,
            "messages": [
                {"role": "system", "content": self.SUPERPROMPT},
                {"role": "user", "content": f"Вот информация для формирования ответа: \n\n {context}\n\n Теперь ответь на следующий вопрос: {query}"}

            ],
            "temperature": 0.0
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(self.url, headers=self.headers, json=data)
                if response.status_code != 200:
                    logger.error("Request to LLM API failed with status code: {}", response.status_code)
                    return LlmResponse(user_id=LlmRequest.user_id, response="NO RESPONSE")
            except Exception as e:
                logger.exception("Exception occurred while requesting an LLM API: {}", e)
                return LlmResponse(user_id=LlmRequest.user_id, response="NO RESPONSE")

        return LlmResponse(
            user_id=LlmRequest.user_id,
            response=response.json()["choices"][0]["message"]["content"]
        )

    async def runloop(self):
        while self.running:
            if not self.__llm_request_queue.empty():
                request = await self.__llm_request_queue.get()
                if type(request) != LlmRequest:
                    logger.warning("Unsupported type of request: {}", type(request))
                    continue

                try:
                    response = await self.__query(request)
                    logger.info("Processed request for user id: {}", response.user_id)
                    await self.llm_response_queue.put(response)
                except Exception as e:
                    logger.exception("Exception occurred while processing request for user id: {}", e)
                finally:
                    self.__llm_request_queue.task_done()

    async def close(self):
        self.running = False
        await self.__llm_request_queue.join()
        await self.llm_response_queue.join()

        try:
            await self.__llm_request_queue
        except asyncio.CancelledError:
            pass

        try:
            await self.llm_response_queue
        except asyncio.CancelledError:
            pass

        logger.info("LLM client closed.")

