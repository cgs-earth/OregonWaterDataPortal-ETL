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
from dagster import get_dagster_logger
from pickle import UnpicklingError
import requests
import shelve
from typing import ClassVar, Optional, Tuple

from userCode.env import RUNNING_AS_TEST_OR_DEV
from userCode.util import deterministic_hash


HEADERS = {"accept": "application/vnd.api+json"}


class ShelveCache:
    """
    Helper cache for storing and fetching data from the oregon API.
    Not used in production, moreso to make testing less intensive on upstream
    """

    db: ClassVar[str] = "oregondb"
    # skip caching in prod and always fetch
    skip_caching_in_prod: bool = False

    def __init__(self, skip_caching_in_prod):
        self.skip_caching_in_prod = skip_caching_in_prod

    def set(self, url: str, content: bytes, _ttl: Optional[timedelta] = None):
        try:
            with shelve.open(ShelveCache.db, "w") as db:
                db[self.hash_url(url)] = content
        except Exception:
            get_dagster_logger().warning(f"Unable to cache: {url}")

    def get_or_fetch(
        self,
        url: str,
        force_fetch: bool,
    ) -> Tuple[bytes, int]:
        # If we are in prod we want to ignore using the cache and not store anything
        runningInProd = not RUNNING_AS_TEST_OR_DEV
        if (self.skip_caching_in_prod and runningInProd) or RUNNING_AS_TEST_OR_DEV:
            response = requests.get(url, headers=HEADERS, timeout=300)
            return response.content, response.status_code

        if self.contains(url) and not force_fetch:
            try:
                return self.get(url), 200
            except (KeyError, UnpicklingError):
                # Force fetch
                return self.get_or_fetch(url, True)
        else:
            res = requests.get(url, headers=HEADERS, timeout=300)
            self.set(url, res.content)
            return res.content, res.status_code

    def reset(self):
        with shelve.open(ShelveCache.db, "w") as db:
            for key in db.keys():
                del db[key]

    def clear(self, url: str):
        url_hash = self.hash_url(url)
        with shelve.open(ShelveCache.db, "w") as db:
            if self.hash_url(url) not in db:
                return

            del db[url_hash]

    def contains(self, url: str) -> bool:
        with shelve.open(ShelveCache.db) as db:
            return self.hash_url(url) in db

    def get(self, url: str):
        with shelve.open(ShelveCache.db) as db:
            return db[self.hash_url(url)]

    @staticmethod
    def hash_url(url: str):
        return str(deterministic_hash(url, 16))
