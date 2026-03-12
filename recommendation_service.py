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
        "als_recommendations.parquet",
        columns=["user_id", "item_id", "score"],
    )
    rec_store.load(
        "default",
        "top_recs.parquet",
        columns=["item_id", "rank"],
    )

    yield

    logger.info("Stopping")
    rec_store.stats()


app = FastAPI(title="recommendations", lifespan=lifespan)


@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    """
    Возвращает список офлайн-рекомендаций длиной k для пользователя user_id
    """
    recs = rec_store.get(user_id, k)
    return {"recs": recs}


def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids


@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):

    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # получаем 3 последних события пользователя
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)
    events = resp.json()["events"]

    items = []
    scores = []

    for item_id in events:

        params = {"item_id": item_id, "k": k}
        resp = requests.post(features_store_url + "/similar_items", headers=headers, params=params)

        item_similar_items = resp.json()

        items += item_similar_items["item_id_2"]
        scores += item_similar_items["score"]

    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    recs = dedup_ids(combined)

    return {"recs": recs[:k]}


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []

    min_length = min(len(recs_offline), len(recs_online))
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        recs_blended.append(recs_online[i])
        recs_blended.append(recs_offline[i])

    # добавляем оставшиеся элементы в конец
    recs_blended += recs_online[min_length:] + recs_offline[min_length:]

    # удаляем дубликаты
    recs_blended = dedup_ids(recs_blended)

    # оставляем только первые k рекомендаций
    recs_blended = recs_blended[:k]

    return {"recs": recs_blended}