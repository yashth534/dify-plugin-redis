from typing import Any

import redis
from redis.client import Redis
from rediscluster import RedisCluster

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 6379
DEFAULT_PORT_SSL = 25061
DEFAULT_DB = 0


def _use_ssl(data: dict[str, Any]) -> bool:
    val = data.get('use_ssl', 'none')
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ('ssl-tls', 'true', '1', 'yes')
    return False


def _resolve_port(data: dict[str, Any], ssl: bool) -> int:
    explicit = data.get('port')
    if explicit:
        return int(explicit)
    return DEFAULT_PORT_SSL if ssl else DEFAULT_PORT


def get_redis_cluster(data: dict[str, Any]) -> RedisCluster:
    ssl = _use_ssl(data)
    port = _resolve_port(data, ssl)

    kwargs: dict[str, Any] = {
        'startup_nodes': [{'host': data.get('host') or DEFAULT_HOST, 'port': port}],
        'password': data.get('password'),
        'decode_responses': True,
    }

    if data.get('username'):
        kwargs['username'] = data['username']

    if ssl:
        kwargs['ssl'] = True
        kwargs['ssl_cert_reqs'] = 'none'
        kwargs['skip_full_coverage_check'] = True

    return RedisCluster(**kwargs)


def get_redis_single(data: dict[str, Any]) -> Redis:
    ssl = _use_ssl(data)
    port = _resolve_port(data, ssl)

    pool_kwargs: dict[str, Any] = {
        'host': data.get('host') or DEFAULT_HOST,
        'port': port,
        'db': int(data.get('db') or DEFAULT_DB),
        'password': data.get('password'),
        'decode_responses': True,
    }

    if data.get('username'):
        pool_kwargs['username'] = data['username']

    if ssl:
        pool_kwargs['connection_class'] = redis.SSLConnection
        pool_kwargs['ssl_cert_reqs'] = None

    return redis.Redis(connection_pool=redis.ConnectionPool(**pool_kwargs))


def get_redis_connection(data: dict[str, Any]) -> RedisCluster | Redis:
    is_cluster = bool(data.get('cluster') or False)
    if is_cluster:
        return get_redis_cluster(data)
    else:
        return get_redis_single(data)
