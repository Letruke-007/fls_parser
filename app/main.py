#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi import status as http_status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from parser import parse_statement
from normalizer import normalize


APP_NAME = "rtf-statement-parser"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "/data")).resolve()
RESULTS_DIR = STORAGE_DIR / "results"
JOBS_DIR = STORAGE_DIR / "jobs"
RETENTION_SECONDS = int(os.getenv("RETENTION_SECONDS", "3600"))
WORKERS = int(os.getenv("WORKERS", "2"))
DEFAULT_CALLBACK_URL = os.getenv("CALLBACK_URL", "").strip() or None
CALLBACK_TIMEOUT_SECONDS = float(os.getenv("CALLBACK_TIMEOUT_SECONDS", "20"))
MAX_FILE_SIZE_BYTES = int(os.getenv("MAX_FILE_SIZE_BYTES", str(10 * 1024 * 1024)))
ALLOWED_ORIGINS_RAW = os.getenv("ALLOWED_ORIGINS", "http://localhost:8000,http://127.0.0.1:8000").strip()

for directory in (RESULTS_DIR, JOBS_DIR):
    directory.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(APP_NAME)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)


def get_allowed_origins() -> List[str]:
    if not ALLOWED_ORIGINS_RAW:
        return []
    values = [item.strip() for item in ALLOWED_ORIGINS_RAW.split(",") if item.strip()]
    return ["*"] if "*" in values else values


class TaskStatus(str, Enum):
    queued = "queued"
    running = "running"
    completed = "completed"
    canceled = "canceled"
    failed = "failed"


@dataclass
class Job:
    id: str
    src_path: Path
    callback_url: Optional[str] = None
    status: TaskStatus = TaskStatus.queued
    error: Optional[str] = None
    result_path: Optional[Path] = None
    created_at: float = field(default_factory=lambda: time.time())
    updated_at: float = field(default_factory=lambda: time.time())
    cancel_flag: bool = False


class StatusResponse(BaseModel):
    id: str
    status: TaskStatus
    error: Optional[str] = None


class ConvertResponse(BaseModel):
    id: str
    status: TaskStatus


class ResultResponse(BaseModel):
    id: str
    status: TaskStatus
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


JOBS: Dict[str, Job] = {}
QUEUE: asyncio.Queue[str] = asyncio.Queue()
WORKER_TASKS: List[asyncio.Task] = []
CLEANER_TASK: Optional[asyncio.Task] = None
STORE_LOCK = asyncio.Lock()


async def run_conversion(job: Job) -> None:
    if job.cancel_flag:
        job.status = TaskStatus.canceled
        job.updated_at = time.time()
        return

    job.status = TaskStatus.running
    job.updated_at = time.time()

    try:
        result = await asyncio.to_thread(parse_statement, job.src_path)
        if job.cancel_flag:
            job.status = TaskStatus.canceled
            job.updated_at = time.time()
            return

        result_path = RESULTS_DIR / f"{job.id}.json"
        await asyncio.to_thread(result_path.write_text, json.dumps(result, ensure_ascii=False), "utf-8")
        job.result_path = result_path
        job.status = TaskStatus.completed
        job.updated_at = time.time()

        callback_url = job.callback_url or DEFAULT_CALLBACK_URL
        if callback_url:
            normalized = normalize(result, source_filename=job.src_path.name)
            payload = {"id": job.id, "status": job.status, "result": normalized}
            try:
                async with httpx.AsyncClient(timeout=CALLBACK_TIMEOUT_SECONDS) as client:
                    response = await client.post(callback_url, json=payload)
                    logger.info("Callback POST %s -> %s", callback_url, response.status_code)
            except Exception:
                logger.exception("Ошибка callback POST на %s", callback_url)
    except asyncio.CancelledError:
        job.status = TaskStatus.canceled
        job.updated_at = time.time()
    except Exception as exc:
        job.status = TaskStatus.failed
        job.error = str(exc)
        job.updated_at = time.time()

        callback_url = job.callback_url or DEFAULT_CALLBACK_URL
        if callback_url:
            payload = {"id": job.id, "status": job.status, "result": None, "error": job.error}
            try:
                async with httpx.AsyncClient(timeout=CALLBACK_TIMEOUT_SECONDS) as client:
                    response = await client.post(callback_url, json=payload)
                    logger.info("Callback POST %s (failed) -> %s", callback_url, response.status_code)
            except Exception:
                logger.exception("Ошибка callback POST (failed) на %s", callback_url)


async def worker_loop(name: str) -> None:
    logger.info("Старт воркера: %s", name)
    while True:
        job_id = await QUEUE.get()
        try:
            async with STORE_LOCK:
                job = JOBS.get(job_id)
            if not job:
                logger.warning("Задача %s не найдена", job_id)
                continue
            if job.cancel_flag:
                job.status = TaskStatus.canceled
                job.updated_at = time.time()
                continue
            await run_conversion(job)
        except Exception:
            logger.exception("Ошибка в воркере при обработке задачи %s", job_id)
        finally:
            QUEUE.task_done()


async def cleanup_loop() -> None:
    while True:
        await asyncio.sleep(60)
        now = time.time()
        expired: List[str] = []
        async with STORE_LOCK:
            for job_id, job in list(JOBS.items()):
                if job.status in (TaskStatus.completed, TaskStatus.failed, TaskStatus.canceled):
                    if now - job.created_at >= RETENTION_SECONDS:
                        expired.append(job_id)

            for job_id in expired:
                job = JOBS.pop(job_id, None)
                if not job:
                    continue
                try:
                    if job.result_path and job.result_path.exists():
                        job.result_path.unlink(missing_ok=True)
                except Exception:
                    logger.warning("Не удалось удалить результат задачи %s", job_id)
                try:
                    shutil.rmtree(JOBS_DIR / job_id, ignore_errors=True)
                except Exception:
                    logger.warning("Не удалось удалить директорию задачи %s", job_id)


app = FastAPI(title="RTF statement parser", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_origins(),
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup() -> None:
    global CLEANER_TASK
    for index in range(max(1, WORKERS)):
        WORKER_TASKS.append(asyncio.create_task(worker_loop(f"worker-{index + 1}")))
    CLEANER_TASK = asyncio.create_task(cleanup_loop())
    logger.info(
        "Сервис запущен. WORKERS=%s, RETENTION_SECONDS=%s, STORAGE_DIR=%s, MAX_FILE_SIZE_BYTES=%s, ALLOWED_ORIGINS=%s",
        WORKERS,
        RETENTION_SECONDS,
        STORAGE_DIR,
        MAX_FILE_SIZE_BYTES,
        get_allowed_origins(),
    )


@app.on_event("shutdown")
async def on_shutdown() -> None:
    for task in WORKER_TASKS:
        task.cancel()
    if CLEANER_TASK:
        CLEANER_TASK.cancel()


@app.post("/api/convert", response_model=ConvertResponse)
async def api_convert(
    file: UploadFile = File(..., description="RTF statement file"),
    callback: Optional[str] = Query(default=None, description="Callback URL для POST результата"),
) -> ConvertResponse:
    filename = file.filename or "input.rtf"
    suffix = Path(filename).suffix.lower()
    if suffix != ".rtf":
        raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST, detail="Поддерживаются только .rtf")

    job_id = str(uuid.uuid4())
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    src_path = job_dir / "source.rtf"

    try:
        if callback and not callback.lower().startswith(("http://", "https://")):
            raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST, detail="callback должен начинаться с http:// или https://")

        content = await file.read()
        if len(content) > MAX_FILE_SIZE_BYTES:
            raise HTTPException(
                status_code=http_status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"Файл превышает лимит {MAX_FILE_SIZE_BYTES} байт",
            )
        src_path.write_bytes(content)
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST, detail=f"Ошибка чтения файла: {exc}") from exc

    job = Job(
        id=job_id,
        src_path=src_path,
        callback_url=(callback.strip() if callback else None),
        status=TaskStatus.queued,
    )
    async with STORE_LOCK:
        JOBS[job_id] = job
    await QUEUE.put(job_id)

    return ConvertResponse(id=job_id, status=TaskStatus.queued)


@app.get("/api/convert/{job_id}/status", response_model=StatusResponse)
async def api_status(job_id: str) -> StatusResponse:
    async with STORE_LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Задача не найдена")
    return StatusResponse(id=job.id, status=job.status, error=job.error)


@app.get("/api/convert/{job_id}/cancel", response_model=ConvertResponse)
async def api_cancel(job_id: str) -> ConvertResponse:
    async with STORE_LOCK:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Задача не найдена")
        if job.status in (TaskStatus.completed, TaskStatus.failed, TaskStatus.canceled):
            return ConvertResponse(id=job.id, status=job.status)
        job.cancel_flag = True
        if job.status == TaskStatus.queued:
            job.status = TaskStatus.canceled
            job.updated_at = time.time()
    return ConvertResponse(id=job_id, status=TaskStatus.canceled)


@app.post("/api/convert/{job_id}", response_model=ResultResponse)
async def api_result(job_id: str) -> ResultResponse:
    async with STORE_LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Задача не найдена")

    if job.status == TaskStatus.completed:
        try:
            if job.result_path and job.result_path.exists():
                data = json.loads(await asyncio.to_thread(job.result_path.read_text, "utf-8"))
                return ResultResponse(id=job.id, status=job.status, result=data)
            return ResultResponse(id=job.id, status=job.status, error="Файл результата отсутствует")
        except Exception as exc:
            return ResultResponse(id=job.id, status=TaskStatus.failed, error=f"Ошибка чтения результата: {exc}")

    if job.status == TaskStatus.failed:
        return ResultResponse(id=job.id, status=job.status, error=job.error or "Ошибка конвертации")

    if job.status == TaskStatus.canceled:
        return ResultResponse(id=job.id, status=job.status, error="Задача отменена")

    return ResultResponse(id=job.id, status=job.status, result=None, error=None)


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"status": "ok", "workers": len(WORKER_TASKS)}
