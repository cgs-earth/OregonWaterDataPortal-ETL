from userCode.cache import RedisCache


def test_cache():
    cache = RedisCache()

    cache.set("test", b"test123")
    assert cache.contains("test")

    assert cache.get("test") == b"test123"

    cache.clear("test")
    assert not cache.contains("test")
