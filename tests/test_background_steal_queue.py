import asyncio
import tempfile
import types
import unittest
from pathlib import Path

from astrbot_plugin_stealer.core.events.background_steal_queue import (
    BackgroundStealQueue,
)


class BackgroundQueueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.processed = []
        self.saved = []
        self.plugin = types.SimpleNamespace()
        self.plugin.base_dir = Path(self.temp_dir.name)
        self.plugin.index_manager = types.SimpleNamespace(
            load_index=self._load_index,
            save_index=self._save_index,
        )

        async def _process_image(
            _event,
            file_path,
            is_temp=True,
            is_platform_emoji=True,
            extra_meta=None,
            to_pending=True,
        ):
            self.processed.append(
                {
                    "event": _event,
                    "file_path": file_path,
                    "to_pending": to_pending,
                    "extra_meta": dict(extra_meta or {}),
                }
            )
            return True, {"processed.png": {"category": "happy"}}

        self.plugin._process_image = _process_image
        self.queue = BackgroundStealQueue(self.plugin, capacity=2, worker_count=1)
        await self.queue.start()

    async def asyncTearDown(self):
        await self.queue.stop()
        self.temp_dir.cleanup()

    async def _load_index(self):
        return {}

    async def _save_index(self, index):
        self.saved.append(dict(index))

    def _write_file(self, name="source.jpg"):
        path = Path(self.temp_dir.name) / name
        path.write_bytes(b"fixture")
        return path

    async def test_capture_returns_before_worker_finishes_and_cleans_file(self):
        source = self._write_file()
        self.plugin.event_handler = types.SimpleNamespace()
        self.plugin.event_handler._download_url_to_temp = self._download_url_to_temp
        self.queue.submit_capture(
            [
                {
                    "media_ref": str(source),
                    "source": "automatic",
                    "to_pending": True,
                    "extra_meta": {"origin_target": "group:242352408"},
                }
            ]
        )
        await asyncio.sleep(0)
        self.assertEqual(self.processed, [])
        for _ in range(20):
            if self.processed:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(len(self.processed), 1)
        self.assertIsNone(self.processed[0]["event"])
        self.assertEqual(self.processed[0]["extra_meta"]["origin_target"], "group:242352408")
        self.assertFalse(Path(self.processed[0]["file_path"]).exists())

    async def test_local_media_is_snapshotted_before_event_release(self):
        source = self._write_file("event-owned.gif")
        self.plugin.event_handler = types.SimpleNamespace(
            _download_url_to_temp=self._download_url_to_temp
        )
        accepted = await self.queue.submit_capture_async(
            [
                {
                    "media_ref": str(source),
                    "source": "automatic",
                    "to_pending": True,
                    "extra_meta": {},
                }
            ]
        )
        source.unlink()
        self.assertTrue(accepted)
        for _ in range(20):
            if self.processed:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(len(self.processed), 1)
        self.assertFalse(Path(self.processed[0]["file_path"]).exists())

    async def _download_url_to_temp(self, _url):
        return None, False

    async def test_shutdown_cleans_pending_staging_files(self):
        self.queue._queue.put_nowait(
            types.SimpleNamespace(
                file_path=str(self._write_file("queued.jpg")),
            )
        )
        await self.queue.stop()
        self.assertFalse(Path(self.temp_dir.name, "queued.jpg").exists())

    async def test_non_pending_result_is_saved(self):
        source = self._write_file("direct.jpg")
        self.plugin.event_handler = types.SimpleNamespace(
            _download_url_to_temp=self._download_url_to_temp
        )
        self.queue.submit_capture(
            [
                {
                    "media_ref": str(source),
                    "source": "force",
                    "to_pending": False,
                    "extra_meta": {},
                }
            ]
        )
        for _ in range(20):
            if self.saved:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(self.saved, [{"processed.png": {"category": "happy"}}])

    async def test_full_queue_drops_newest_staged_file(self):
        for worker in self.queue._workers:
            worker.cancel()
        await asyncio.gather(*self.queue._workers, return_exceptions=True)
        self.queue._workers.clear()
        self.plugin.event_handler = types.SimpleNamespace(
            _download_url_to_temp=self._download_url_to_temp
        )
        first = self._write_file("first.jpg")
        second = self._write_file("second.jpg")
        self.queue.capacity = 1
        self.queue._queue = asyncio.Queue(maxsize=1)
        await self.queue._stage_and_enqueue(
            [
                {"media_ref": str(first), "source": "automatic"},
                {"media_ref": str(second), "source": "automatic"},
            ]
        )
        self.assertEqual(self.queue.pending_count, 1)
        self.assertEqual(len(list(self.queue.staging_dir.iterdir())), 1)
