import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
from cachetools import LRUCache
import logging
from dataclasses import dataclass
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass
class CacheEntry:
    content: bytes
    etag: Optional[str] = None
    last_modified: Optional[str] = None


class Settings(BaseSettings):
    UPSTREAM_MIRROR_URL: str = "https://mirrors.tuna.tsinghua.edu.cn/"
    CACHE_MAX_SIZE: int = 128

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fast-pkg-proxy")


app = FastAPI(
    title="Fast Package Proxy",
    description="A caching proxy for Linux package repositories with in-memory caching.",
    version="0.2.0",
)


cache = LRUCache(maxsize=settings.CACHE_MAX_SIZE)


async def check_if_modified(upstream_url: str, cache_entry: CacheEntry) -> bool:
    async with httpx.AsyncClient() as client:
        try:
            headers = {}
            if cache_entry.etag:
                headers["If-None-Match"] = cache_entry.etag
            if cache_entry.last_modified:
                headers["If-Modified-Since"] = cache_entry.last_modified

            response = await client.head(upstream_url, headers=headers, timeout=10)
            return response.status_code != 304
        except Exception as e:
            logger.warning(f"Failed to check modification for {upstream_url}: {e}")
            return True


async def download_and_cache_streaming(upstream_url: str, cache_key: str):
    logger.info(
        f"Cache miss for '{cache_key}'. Streaming download from: {upstream_url}"
    )

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                upstream_url,
                follow_redirects=True,
                headers={"User-Agent": "Fast-Package-Proxy/0.2.0"},
                timeout=None,
            )
            response.raise_for_status()

            chunks = []
            async for chunk in response.aiter_bytes():
                chunks.append(chunk)
                yield chunk

            content = b"".join(chunks)
            cache_entry = CacheEntry(
                content=content,
                etag=response.headers.get("ETag"),
                last_modified=response.headers.get("Last-Modified"),
            )
            cache[cache_key] = cache_entry
            logger.info(f"Successfully cached streamed content for: {cache_key}")

        except httpx.HTTPStatusError as e:
            logger.error(f"Upstream error for {upstream_url}: {e.response.status_code}")
            error_message = (
                f"Error {e.response.status_code}: File not found or upstream error"
            )
            yield error_message.encode()
            return
        except Exception as e:
            logger.error(f"Error during streaming for {upstream_url}: {e}")
            error_message = "Error: Failed to download file"
            yield error_message.encode()
            return


@app.get("/{full_path:path}")
async def get_package(request: Request, full_path: str):
    cache_key = full_path
    upstream_url = f"{settings.UPSTREAM_MIRROR_URL.rstrip('/')}/{full_path}"

    if cache_key in cache:
        cache_entry = cache[cache_key]

        if isinstance(cache_entry, CacheEntry) and (
            cache_entry.etag or cache_entry.last_modified
        ):
            is_modified = await check_if_modified(upstream_url, cache_entry)
            if not is_modified:
                logger.info(f"Cache hit (not modified) for: {cache_key}")
                return Response(content=cache_entry.content)
            else:
                logger.info(f"Cache invalidated (modified) for: {cache_key}")
                del cache[cache_key]
        else:
            logger.info(f"Cache hit for: {cache_key}")
            content = (
                cache_entry.content
                if isinstance(cache_entry, CacheEntry)
                else cache_entry
            )
            return Response(content=content)

    generator = download_and_cache_streaming(upstream_url, cache_key)
    return StreamingResponse(generator)
