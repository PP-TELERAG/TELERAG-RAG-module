from typing import List
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer
from loguru import logger
class Chunker:
    def __init__(self, model: str, max_tokens:int = 128):
        self.model_name = model
        self.model = SentenceTransformer(model)
        self.tokenizer = AutoTokenizer.from_pretrained(model)
        self.max_tokens = max_tokens

    async def chunk_text(self, text: str) -> List[str]:

        sentences = text.split(". ")
        chunks = []
        current_chunk = ""
        logger.info("Tokenizing...")
        for sentence in tqdm(sentences):
            sentence_tokens = self.tokenizer.tokenize(sentence)
            current_tokens = self.tokenizer.tokenize(current_chunk)

            if len(current_tokens) + len(sentence_tokens) <= self.max_tokens:
                if current_chunk:
                    current_chunk += ". " + sentence
                else:
                    current_chunk = sentence

            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = sentence

            if current_chunk:
                chunks.append(current_chunk)
            logger.info("Tokenized!")
            return chunks

    async def get_embeddings(self, chunks: List[str]) -> List[List[float]]:
        logger.info("Getting embeddings...")
        return self.model.encode(chunks).tolist()



