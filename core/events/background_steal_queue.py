"""Bounded background pipeline for passive meme stealing."""

from __future__ import annotations

import asyncio
import os
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from astrbot.api import logger

from ..util.safe_io import safe_remove_file


@dataclass(slots=True)
class StealJob:
    """A serializable processing job; event objects never cross this boundary."""

    file_path: str
    extra_meta: dict[str, Any] = field(default_factory=dict)
    to_pending: bool = True
    source: str = "automatic"


class BackgroundStealQueue:
    """Bounded queue and workers for downloaded/staged meme files."""

    def __init__(self, plugin: Any, *, capacity: int = 32, worker_count: int = 2):
        self.plugin = plugin
        self.capacity = max(1, int(capacity))
        self.worker_count = max(1, int(worker_count))
        base_dir = getattr(plugin, "base_dir", None)
        self.staging_dir = Path(base_dir or Path.cwd()) / "staging"
        self._queue: asyncio.Queue[StealJob] = asyncio.Queue(maxsize=self.capacity)
        self._workers: list[asyncio.Task[Any]] = []
        self._capture_tasks: set[asyncio.Task[Any]] = set()
        self._save_lock = asyncio.Lock()
        self._accepting = False

    @property
    def pending_count(self) -> int:
        return self._queue.qsize()

    async def start(self) -> None:
        if self._accepting:
            return
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self._accepting = True
        self._workers = [
            asyncio.create_task(self._worker_loop(), name=f"steal_worker_{i}")
            for i in range(self.worker_count)
        ]
        logger.info(
            f"[Stealer] 后台偷图队列已启动: capacity={self.capacity}, workers={self.worker_count}"
        )

    async def stop(self) -> None:
        self._accepting = False
        for task in list(self._capture_tasks):
            if not task.done():
                task.cancel()
        if self._capture_tasks:
            await asyncio.gather(*self._capture_tasks, return_exceptions=True)
        self._capture_tasks.clear()

        for task in self._workers:
            if not task.done():
                task.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

        while not self._queue.empty():
            try:
                job = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            await safe_remove_file(job.file_path)
            self._queue.task_done()

        if self.staging_dir.exists():
            for path in self.staging_dir.iterdir():
                if path.is_file():
                    await safe_remove_file(str(path))
        logger.info("[Stealer] 后台偷图队列已停止，暂存文件已清理")

    def submit_capture(self, descriptors: list[dict[str, Any]]) -> bool:
        """Schedule staging/download work and return without waiting for it."""
        if not self._accepting or not descriptors:
            return False
        task = asyncio.create_task(
            self._stage_and_enqueue(descriptors), name="steal_stage_enqueue"
        )
        self._capture_tasks.add(task)
        task.add_done_callback(self._capture_done)
        return True

    async def submit_capture_async(self, descriptors: list[dict[str, Any]]) -> bool:
        """Snapshot local media before the originating event is released.

        AstrBot owns local media paths only for the lifetime of the event. Local
        files are copied into plugin staging here; remote URLs remain detached
        background downloads so network latency never reaches the event handler.
        """
        if not self._accepting or not descriptors:
            return False
        local_descriptors: list[dict[str, Any]] = []
        remote_descriptors: list[dict[str, Any]] = []
        for descriptor in descriptors:
            ref = str(descriptor.get("media_ref") or "")
            if ref.startswith(("http://", "https://")):
                remote_descriptors.append(descriptor)
            else:
                local_descriptors.append(descriptor)

        accepted = False
        for descriptor in local_descriptors:
            staged = await self._stage_local_descriptor(descriptor)
            if staged:
                accepted = True
        if remote_descriptors:
            accepted = self.submit_capture(remote_descriptors) or accepted
        return accepted

    async def _stage_local_descriptor(self, descriptor: dict[str, Any]) -> bool:
        if not self._accepting:
            return False
        ref = str(descriptor.get("media_ref") or "")
        staged_path = ""
        try:
            if not ref or not os.path.exists(ref):
                logger.warning(f"后台偷图暂存失败：媒体不存在 ref={ref[:120]}")
                return False
            suffix = Path(ref).suffix or ".jpg"
            staged = self.staging_dir / f"{uuid.uuid4().hex}{suffix.lower()}"
            await asyncio.to_thread(shutil.copy2, ref, staged)
            staged_path = str(staged)
            job = StealJob(
                file_path=staged_path,
                extra_meta=dict(descriptor.get("extra_meta") or {}),
                to_pending=bool(descriptor.get("to_pending", True)),
                source=str(descriptor.get("source") or "automatic"),
            )
            if self._queue.full():
                await safe_remove_file(staged_path)
                logger.warning(
                    "[Stealer] 后台偷图队列已满，丢弃最新任务 "
                    f"source={job.source} meta={job.extra_meta}"
                )
                return False
            self._queue.put_nowait(job)
            logger.info(
                f"[Stealer] 后台偷图已入队 source={job.source}, queued={self._queue.qsize()}"
            )
            return True
        except Exception as exc:
            logger.error(f"后台偷图本地暂存失败 ref={ref[:120]}: {exc!r}", exc_info=exc)
            if staged_path:
                await safe_remove_file(staged_path)
            return False

    def _capture_done(self, task: asyncio.Task[Any]) -> None:
        self._capture_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(f"后台偷图暂存任务异常: {exc!r}", exc_info=exc)

    async def _stage_and_enqueue(self, descriptors: list[dict[str, Any]]) -> None:
        handler = getattr(self.plugin, "event_handler", None)
        if handler is None:
            return
        for descriptor in descriptors:
            if not self._accepting:
                return
            source_path = ""
            source_is_temp = False
            try:
                ref = str(descriptor.get("media_ref") or "")
                if ref.startswith(("http://", "https://")):
                    source_path, _ = await handler._download_url_to_temp(ref)
                    source_is_temp = True
                elif ref and os.path.exists(ref):
                    source_path = ref
                if not source_path or not os.path.exists(source_path):
                    logger.warning(f"后台偷图暂存失败：媒体不存在 ref={ref[:120]}")
                    continue

                suffix = Path(source_path).suffix or ".jpg"
                staged_path = self.staging_dir / f"{uuid.uuid4().hex}{suffix.lower()}"
                await asyncio.to_thread(shutil.copy2, source_path, staged_path)
                if source_is_temp:
                    await safe_remove_file(source_path)

                job = StealJob(
                    file_path=str(staged_path),
                    extra_meta=dict(descriptor.get("extra_meta") or {}),
                    to_pending=bool(descriptor.get("to_pending", True)),
                    source=str(descriptor.get("source") or "automatic"),
                )
                if self._queue.full():
                    await safe_remove_file(job.file_path)
                    logger.warning(
                        "[Stealer] 后台偷图队列已满，丢弃最新任务 "
                        f"source={job.source} meta={job.extra_meta}"
                    )
                    continue
                self._queue.put_nowait(job)
                logger.info(
                    f"[Stealer] 后台偷图已入队 source={job.source}, queued={self._queue.qsize()}"
                )
            except asyncio.CancelledError:
                if source_is_temp and source_path:
                    await safe_remove_file(source_path)
                raise
            except Exception as exc:
                logger.error(f"后台偷图暂存失败: {exc!r}", exc_info=exc)
                if source_is_temp and source_path:
                    await safe_remove_file(source_path)

    async def _worker_loop(self) -> None:
        while True:
            job = await self._queue.get()
            try:
                success, index = await self.plugin._process_image(
                    None,
                    job.file_path,
                    is_temp=True,
                    is_platform_emoji=True,
                    extra_meta=job.extra_meta,
                    to_pending=job.to_pending,
                )
                logger.info(
                    f"[Stealer] 后台偷图处理完成 source={job.source}, "
                    f"success={success}, pending={job.to_pending}"
                )
                if success and isinstance(index, dict) and not job.to_pending:
                    async with self._save_lock:
                        current = await self.plugin.index_manager.load_index()
                        current = dict(current or {})
                        current.update(index)
                        await self.plugin.index_manager.save_index(current)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    f"后台偷图任务失败 source={job.source} meta={job.extra_meta}: {exc!r}",
                    exc_info=exc,
                )
            finally:
                await safe_remove_file(job.file_path)
                self._queue.task_done()
