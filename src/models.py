from pydantic import BaseModel
from typing import List

class LlmRequest(BaseModel):
    user_id: int
    context: List[str]
    query: str


class LlmResponse(BaseModel):
    user_id: int
    response: str

class ChromaDBSearchQuery(BaseModel):
    user_id: int
    query: str
    channel_ids: List[int]
    top_k: int

class ChromaDBSearchResponse(BaseModel):
    user_id: int
    query: str
    response: List[str]

class ChromaDBAddDocumentRequest(BaseModel):
    channel_id: int = 0
    document_id: str = str(channel_id)
    text: str

