import logging as logger
import pandas as pd
from fastapi import FastAPI


class Recommendations:

    def __init__(self):
        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }

    def load(self, type, path, **kwargs):

        logger.info(f"Loading recommendations, type: {type}")
        self._recs[type] = pd.read_parquet(path, **kwargs)

        if type == "personal":
            self._recs[type] = self._recs[type].set_index("user_id")

        logger.info("Loaded")

    def get(self, user_id: int, k: int = 100):

        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
        except KeyError:
            recs = self._recs["default"]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except Exception:
            logger.error("No recommendations found")
            recs = []

        return recs


rec_store = Recommendations()

rec_store.load(
    "personal",
    "als_recommendations.parquet",
    columns=["user_id", "item_id", "score"],
)

rec_store.load(
    "default",
    "top_recs.parquet",
    columns=["item_id", "rank"],
)

app = FastAPI()


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):

    recs = rec_store.get(user_id, k)

    return {"recs": recs}
