from pydantic import BaseSettings, Field


class Configuration(BaseSettings):

    LLM_MODEL_NAME: str
    LLM_API_KEY: str
    LLM_API_URL: str
    LLM_SUPERPROMPT: str
    CHROMA_DB_PATH: str
    CHROMA_DB_COLLECTION_NAME: str = "default"
    SENTENCE_TRANSFORMERS_MODEL_NAME: str

    # LOGGING SETTINGS
    LOG_LEVEL: str = "INFO"
    LOGGER_ROTATION: str = "1 MB"
    LOGGER_RETENTION: str = "1 day"
    LOGGER_ENCODING: str = "utf-8"

    # BACKEND SETTINGS
    SERVER_HOST: str = "0.0.0.0"
    SERVER_PORT: int = 8080

    # BROKER RELATED
    BROKER_URL: str
    BROKER_DOC_TOPIC: str
    BROKER_SEARCH_IN_TOPIC: str
    BROKER_SEARCH_OUT_TOPIC: str
    class Config:
        env_file = ".env"


def get_config():
    return Configuration()