# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
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


HEADERS = {"accept": "application/vnd.api+json"}


class ShelveCache:
    """
    Helper cache for storing and fetching data from the oregon API.
    Not used in production, moreso to make testing less intensive on upstream
    """

    db: ClassVar[str] = "oregondb"

    def set(self, url: str, content: bytes, _ttl: Optional[timedelta] = None):
        try:
            with shelve.open(ShelveCache.db, "w") as db:
                db[url] = content
        except Exception:
            get_dagster_logger().warning(f"Unable to cache: {url}")

    def get_or_fetch(self, url: str, force_fetch: bool = False) -> Tuple[bytes, int]:
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
            for key in db:
                del db[key]

    def clear(self, url: str):
        with shelve.open(ShelveCache.db, "w") as db:
            if url not in db:
                return

            del db[url]

    def contains(self, url: str) -> bool:
        with shelve.open(ShelveCache.db) as db:
            return url in db

    def get(self, url: str):
        with shelve.open(ShelveCache.db) as db:
            return db[url]
