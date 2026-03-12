import logging
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI


logger = logging.getLogger("uvicorn.error")


class SimilarItems:

    def __init__(self):
        self._similar_items = None

    def load(self, path, **kwargs):
        """
        Загружаем данные из файла
        """

        logger.info(f"Loading data")
        self._similar_items = pd.read_parquet(path, **kwargs)
        self._similar_items = self._similar_items.set_index("item_id_1").sort_values(
            ["item_id_1", "score"], ascending=[True, False]
        )
        logger.info("Loaded")

    def get(self, item_id: int, k: int = 10):
        """
        Возвращает список похожих объектов
        """
        try:
            i2i = self._similar_items.loc[item_id].head(k)
            i2i = i2i[["item_id_2", "score"]].to_dict(orient="list")
        except KeyError:
            logger.error("No recommendations found")
            i2i = {"item_id_2": [], "score": []}

        return i2i


sim_items_store = SimilarItems()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    sim_items_store.load(
        "./similar_items.parquet",
        columns=["item_id_1", "item_id_2", "score"],
    )
    logger.info("Ready!")
    yield


# создаём приложение FastAPI
app = FastAPI(title="features", lifespan=lifespan)


@app.post("/similar_items")
async def recommendations(item_id: int, k: int = 10):
    """
    Возвращает список похожих объектов длиной k для item_id
    """

    i2i = sim_items_store.get(item_id, k)

    return i2i
