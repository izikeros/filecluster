"""Tests for content-level media integrity checks."""

import subprocess

from PIL import Image

from filecluster import media_integrity
from filecluster.media_integrity import (
    IntegrityStatus,
    verify_image,
    verify_video,
)


def _write_jpeg(path):
    Image.new("RGB", (32, 32), (120, 90, 60)).save(path, "JPEG")
    return path


class TestVerifyImage:
    def test_valid_jpeg_is_ok(self, tmp_path):
        path = _write_jpeg(tmp_path / "good.jpg")
        assert verify_image(path) is IntegrityStatus.OK

    def test_truncated_jpeg_is_corrupt(self, tmp_path):
        path = _write_jpeg(tmp_path / "cut.jpg")
        data = path.read_bytes()
        path.write_bytes(data[: len(data) // 2])
        assert verify_image(path) is IntegrityStatus.CORRUPT

    def test_garbage_content_is_corrupt(self, tmp_path):
        path = tmp_path / "junk.jpg"
        path.write_bytes(b"not an image at all")
        assert verify_image(path) is IntegrityStatus.CORRUPT

    def test_restores_pillow_globals(self, tmp_path):
        from PIL import Image as PILImage
        from PIL import ImageFile

        prev_trunc = ImageFile.LOAD_TRUNCATED_IMAGES
        prev_max = PILImage.MAX_IMAGE_PIXELS
        verify_image(_write_jpeg(tmp_path / "g.jpg"))
        assert prev_trunc == ImageFile.LOAD_TRUNCATED_IMAGES
        assert prev_max == PILImage.MAX_IMAGE_PIXELS


class TestVerifyVideo:
    def test_skipped_when_ffprobe_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(media_integrity, "ffprobe_available", lambda: False)
        path = tmp_path / "clip.mp4"
        path.write_bytes(b"data")
        assert verify_video(path) is IntegrityStatus.SKIPPED

    def test_ok_when_ffprobe_clean(self, tmp_path, monkeypatch):
        monkeypatch.setattr(media_integrity, "ffprobe_available", lambda: True)

        def fake_run(*args, **kwargs):
            return subprocess.CompletedProcess(args, 0, stdout="{}", stderr="")

        monkeypatch.setattr(media_integrity.subprocess, "run", fake_run)
        path = tmp_path / "clip.mp4"
        path.write_bytes(b"data")
        assert verify_video(path) is IntegrityStatus.OK

    def test_corrupt_on_nonzero_exit(self, tmp_path, monkeypatch):
        monkeypatch.setattr(media_integrity, "ffprobe_available", lambda: True)

        def fake_run(*args, **kwargs):
            return subprocess.CompletedProcess(args, 1, stdout="", stderr="boom")

        monkeypatch.setattr(media_integrity.subprocess, "run", fake_run)
        path = tmp_path / "clip.mp4"
        path.write_bytes(b"data")
        assert verify_video(path) is IntegrityStatus.CORRUPT

    def test_corrupt_on_stderr_diagnostics(self, tmp_path, monkeypatch):
        monkeypatch.setattr(media_integrity, "ffprobe_available", lambda: True)

        def fake_run(*args, **kwargs):
            return subprocess.CompletedProcess(
                args, 0, stdout="{}", stderr="moov atom not found"
            )

        monkeypatch.setattr(media_integrity.subprocess, "run", fake_run)
        path = tmp_path / "clip.mp4"
        path.write_bytes(b"data")
        assert verify_video(path) is IntegrityStatus.CORRUPT

    def test_unreadable_on_timeout(self, tmp_path, monkeypatch):
        monkeypatch.setattr(media_integrity, "ffprobe_available", lambda: True)

        def fake_run(*args, **kwargs):
            raise subprocess.TimeoutExpired(cmd="ffprobe", timeout=1)

        monkeypatch.setattr(media_integrity.subprocess, "run", fake_run)
        path = tmp_path / "clip.mp4"
        path.write_bytes(b"data")
        assert verify_video(path) is IntegrityStatus.UNREADABLE
