import httpx
from loguru import logger
from models import LlmRequest, LlmResponse


class LLMClient:
    def __init__(self, api_key: str, api_url: str, model_name: str, superprompt: str, temperature: float):
        self.temperature = temperature
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.url = api_url
        self.MODEL_NAME = model_name
        self.SUPERPROMPT = superprompt

        self.running = True


    async def query(self, request: LlmRequest) -> LlmResponse:
        context = "\n".join(request.context)
        query = LlmRequest.query

        data = {
            "model": self.MODEL_NAME,
            "messages": [
                {"role": "system", "content": self.SUPERPROMPT},
                {"role": "user", "content": f"Вот информация для формирования ответа: \n\n {context}\n\n Теперь ответь на следующий вопрос: {query}"}

            ],
            "temperature": self.temperature,
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



