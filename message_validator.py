from pydantic import BaseModel, ValidationError #библиотека для валидации данных через аннотации типов
from typing import Optional, Dict #библиотека для указания типов

class PayloadModel(BaseModel):
    query: Optional[str] = None  #текст запроса
    context: Optional[str] = None  #доп контекст
    texts: Optional[str] = None  #альтернативное поле для текстовых данных

#Модель для валидации содержания сообщения
class BodyModel(BaseModel):
    userId: int
    channelId: int
    payload: PayloadModel  #Вложенная модель данных

#Класс для валидации сообщений брокера по единому формату
class BrokerMessageValidator:

#Основной метод валидации входящего сообщения
    @classmethod
    def validate(cls, raw_data: Dict) -> Dict:
        try:
            #Проверка наличия корневого поля 'Message'
            if "Сообщение" not in raw_data:
                raise ValueError("Отсутствует поле Сообщение")

            message = raw_data["Сообщение"]

            #Валидация структуры 'body' через Pydantic
            body = BodyModel(**message["body"])

            #Возврат нормализованных данных
            return {
                "method": message.get("method"),  #Опциональный метод обработки
                "subject": message.get("subject"),  #Опциональная тема сообщения
                "body": body.dict()  #Преобразование модели в словарь
            }

        except ValidationError as ve:
            #Ошибка валидации Pydantic (несоответствие типам/структуре)
            raise ValueError(f"Ошибка валидации: {str(ve)}")
        except Exception as e:
            #Любые другие ошибки (например, KeyError)
            raise ValueError(f"Непредвиденная ошибка: {str(e)}")