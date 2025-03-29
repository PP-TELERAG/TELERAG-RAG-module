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

        self.__document_queue = asyncio.Queue()
        self.__search_query_queue = asyncio.Queue()
        self.search_responses_queue = asyncio.Queue()
        self.running = True

    async def put_to_search_queue(self, request: ChromaDBSearchQuery):
        await self.__search_query_queue.put(request)

    async def put_to_document_queue(self, request: ChromaDBAddDocumentRequest):
        await self.__document_queue.put(request)


    async def __add_document(self, request: ChromaDBAddDocumentRequest):
        chunks = await self.Chunker.chunk_text(request.text)
        embeddings = await self.Chunker.get_embeddings(chunks)

        ids = [f"{request.document_id}:{i}" for i in range(len(chunks))]
        metadatas = [{"text": chunk} for chunk in chunks]

        self.collection.add(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
        )
        return ids

    async def __search(self, request: ChromaDBSearchQuery):
        query_embedding = self.Chunker.model.encode(request.query).tolist()
        result = self.collection.query(query_embeddings=[query_embedding], n_results=request.top_k)
        texts = [metadata['text'] for metadata in result['metadatas'][0]]
        await self.search_responses_queue.put(ChromaDBSearchResponse(user_id=request.user_id, query=request.query, response=texts))

    async def runloop(self):
        while self.running:
            if not self.__search_query_queue.empty():
                request = await self.__search_query_queue.get()
                if type(request) is not ChromaDBSearchQuery:
                    logger.warning("Presented search query type is not supported: {}", type(request))
                    self.__search_query_queue.task_done()
                else:
                    try:
                        await self.__search(request)
                        logger.info("Processed search query")
                    except Exception as e:
                        logger.exception("Failed to process search query: {}", e)
                    finally:
                        self.__search_query_queue.task_done()


            if not self.__document_queue.empty():
                request = await self.__document_queue.get()
                if type(request) is not ChromaDBAddDocumentRequest:
                    logger.warning("Presented document addition request type is not supported: {}", type(request))
                    self.__document_queue.task_done()
                else:
                    try:
                        await self.__add_document(request)
                        logger.info("Processed document addition request")
                    except Exception as e:
                        logger.exception("Failed to process document addition request: {}", e)
                    finally:
                        self.__document_queue.task_done()

    async def close(self):
        self.running = False
        await self.__search_query_queue.join()
        await self.__document_queue.join()
        await self.search_responses_queue.join()

        try:
            await self.__search_query_queue
        except asyncio.CancelledError:
            pass

        try:
            await self.__document_queue
        except asyncio.CancelledError:
            pass

        try:
            await self.search_responses_queue
        except asyncio.CancelledError:
            pass

        logger.info("ChromaDB client closed.")




