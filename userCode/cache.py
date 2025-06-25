# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from datetime import timedelta
import json
import os
from dagster import get_dagster_logger
import redis
import requests
from typing import Optional, Tuple

from userCode.env import RUNNING_AS_TEST_OR_DEV
from userCode.util import deterministic_hash


HEADERS = {"accept": "application/vnd.api+json"}

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


class RedisCache:
    """
    Helper cache for storing and fetching data from the Oregon API.
    """

    def __init__(self):
        self.db = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)

    def set(self, url: str, content: bytes, _ttl: Optional[timedelta] = None):
        try:
            key = self.hash_url(url)
            if _ttl:
                self.db.setex(key, int(_ttl.total_seconds()), content)
            else:
                self.db.set(key, content)
        except Exception:
            get_dagster_logger().warning(f"Unable to cache: {url}")

    def get_or_fetch(
        self,
        url: str,
        force_fetch: bool,
        cache_result: bool = RUNNING_AS_TEST_OR_DEV(),
    ) -> Tuple[bytes, int]:
        if not cache_result:
            response = requests.get(url, headers=HEADERS, timeout=300)
            return response.content, response.status_code

        if self.contains(url) and not force_fetch:
            try:
                return self.get(url), 200
            except (KeyError, json.JSONDecodeError):
                return self.get_or_fetch(url, True, cache_result)

        response = requests.get(url, headers=HEADERS, timeout=300)
        self.set(url, response.content)
        return response.content, response.status_code

    def reset(self):
        self.db.flushdb()

    def clear(self, url: str):
        self.db.delete(self.hash_url(url))

    def contains(self, url: str) -> bool:
        return self.db.exists(self.hash_url(url)) == 1

    def get(self, url: str) -> bytes:
        data = self.db.get(self.hash_url(url))
        if data is None:
            raise KeyError(f"{url} not found in cache")
        return data  # type: ignore not clear why this is needed; redis returns bytes

    @staticmethod
    def hash_url(url: str) -> str:
        return str(deterministic_hash(url, 16))
