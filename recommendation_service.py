import logging as logger
from contextlib import asynccontextmanager

import pandas as pd
import requests
from fastapi import FastAPI


features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020"


class Recommendations:
    def __init__(self):
        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
            "request_online_count": 0,
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
            recs = self._recs["default"]["item_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except Exception:
            logger.error("No recommendations found")
            recs = []

        return recs

    def stats(self):
        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value}")


rec_store = Recommendations()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting")

    rec_store.load(
        "personal",
        "goodread/als_recommendations.parquet",
        columns=["user_id", "item_id", "score"],
    )
    rec_store.load(
        "default",
        "goodread/top_recs.parquet",
        columns=["item_id", "rank"],
    )

    yield

    logger.info("Stopping")
    rec_store.stats()


app = FastAPI(title="recommendations", lifespan=lifespan)


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    recs = rec_store.get(user_id, k)
    return {"recs": recs}


@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 10):
    """
    Возвращает онлайн-рекомендации по последнему событию пользователя
    """
    try:
        events_resp = requests.post(
            events_store_url + "/get",
            params={"user_id": user_id, "k": 1},
            timeout=5,
        )
        events_resp.raise_for_status()
        events = events_resp.json().get("events", [])

        if not events:
            return {"recs": []}

        last_item_id = events[0]

        sim_resp = requests.post(
            features_store_url + "/similar_items",
            params={"item_id": last_item_id, "k": k},
            timeout=5,
        )
        sim_resp.raise_for_status()
        similar_items = sim_resp.json().get("item_id_2", [])

        rec_store._stats["request_online_count"] += 1
        return {"recs": similar_items}

    except Exception as e:
        logger.error(f"Online recommendations error: {e}")
        return {"recs": []}