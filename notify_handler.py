from fastapi import APIRouter, HTTPException
from ..validators.message_validator import BrokerMessageValidator
#Импорт валидатора сообщений
router = APIRouter()  #Создание роутера для эндпоинтов


@router.post("/notify")
async def unified_notification_handler(request_data: dict):
    try:
        #Валидация входящего сообщения
        validated = BrokerMessageValidator.validate(request_data)

        #Маршрутизация по типу сообщения
        if validated["subject"] == "rag_request":
            return await _handle_rag(validated)  #Обработчик RAG-запросов
        elif validated["method"] == "status_update":
            return await _handle_status(validated)  # бработчик статусов

        #Если тип не поддерживается
        raise HTTPException(
            status_code=400,
            detail="Unsupported message type"
        )

    except ValueError as ve:
        #Ошибка валидации
        raise HTTPException(
            status_code=422,
            detail=str(ve)
        )
    except Exception as e:
        #Любая другая ошибка
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )
#Примечание: Функции _handle_rag и _handle_status должны быть реализованы отдельно.
