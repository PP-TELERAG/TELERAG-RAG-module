import asyncio
from models import ChromaDBSearchQuery, ChromaDBSearchResponse, ChromaDBAddDocumentRequest
import chromadb as cdb
from chunker import Chunker
from loguru import logger
class ChromaDBClient:
    def __init__(self, db_path: str, collection_name: str, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        self.Chunker = Chunker(model_name)
        self.client = cdb.PersistentClient(path=db_path)
        self.collection = self.client.get_collection(collection_name)


    async def add_document(self, request: ChromaDBAddDocumentRequest):
        chunks = await self.Chunker.chunk_text(request.text)
        embeddings = await self.Chunker.get_embeddings(chunks)

        ids = [f"{request.document_id}:{i}" for i in range(len(chunks))]
        metadata = [{"channel_id": request.channel_id}]

        self.collection.add(
            ids=ids,
            documents=chunks,
            embeddings=embeddings,
            metadatas=metadata,
        )
        return ids

    async def search(self, request: ChromaDBSearchQuery) -> ChromaDBSearchResponse:
        query_embedding = self.Chunker.model.encode(request.query).tolist()
        where_filter = ({"channel_id": {"$in": request.channel_ids}})
        result = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=request.top_k,
            where=where_filter
        )
        texts = result['documents'][0]

        return ChromaDBSearchResponse(
            user_id=request.user_id,
            query=request.query,
            response=texts,
        )











