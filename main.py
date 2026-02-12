import os
import zlib
import struct
import psutil
import asyncio
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import StreamingResponse

CHUNK_SIZE = 1024 * 64
MAX_MEMORY_MB = 512

app = FastAPI(title="Fast Noise PNG Generator")

stats = {
    "total_requests": 0,
    "running_requests": 0
}


def get_current_memory_usage() -> int:
    process = psutil.Process(os.getpid())
    # Используем RSS (Resident Set Size) для получения реальной физической памяти
    return process.memory_info().rss


def check_memory_limit():
    if MAX_MEMORY_MB > 0:
        usage_mb = get_current_memory_usage() / (1024 * 1024)
        if usage_mb > MAX_MEMORY_MB:
            raise HTTPException(
                status_code=503,
                detail=f"Service memory limit reached ({usage_mb:.1f}MB > {MAX_MEMORY_MB}MB)"
            )


def create_png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    """Стандартная структура чанка PNG: [Length][Type][Data][CRC]"""
    length = struct.pack("!I", len(data))
    crc = struct.pack("!I", zlib.crc32(chunk_type + data) & 0xFFFFFFFF)
    return length + chunk_type + data + crc


async def generate_random_png(width: int, height: int):
    stats["running_requests"] += 1

    try:
        # Сигнатура PNG и IHDR (8-bit RGB)
        yield b"\x89PNG\r\n\x1a\n"
        ihdr_data = struct.pack("!IIBBBBB", width, height, 8, 2, 0, 0, 0)
        yield create_png_chunk(b"IHDR", ihdr_data)

        # Потоковое сжатие данных с помощью zlib.compressobj
        compressor = zlib.compressobj(level=6)

        for _ in range(height):
            # Каждая строка PNG начинается с байта фильтрации (0x00 - фильтр отсутствует)
            row_raw_data = b"\x00" + os.urandom(width * 3)

            compressed_part = compressor.compress(row_raw_data)
            if compressed_part:
                yield create_png_chunk(b"IDAT", compressed_part)

            # Принудительная передача управления event loop для неблокирующей обработки
            await asyncio.sleep(0)

        final_compressed_data = compressor.flush()
        if final_compressed_data:
            yield create_png_chunk(b"IDAT", final_compressed_data)

        yield create_png_chunk(b"IEND", b"")

    finally:
        stats["running_requests"] -= 1


@app.get("/health")
async def health():
    return {
        "memory": get_current_memory_usage(),
        "requests": {
            "total": stats["total_requests"],
            "running": stats["running_requests"]
        }
    }


@app.get("/generate")
async def generate(
        w: int = Query(..., gt=0),
        h: int = Query(..., gt=0)
):
    check_memory_limit()
    stats["total_requests"] += 1

    return StreamingResponse(
        generate_random_png(w, h),
        media_type="image/png"
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)