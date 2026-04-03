import os
from unittest.mock import MagicMock, patch, call

from humblebundle_downloader.download_library import DownloadLibrary


###
# _should_download_file_type
###
def test_include_logic_has_values():
    dl = DownloadLibrary(
        "fake_library_path",
        ext_include=["pdf", "EPub"],
    )
    assert dl._should_download_file_type("pdf") is True
    assert dl._should_download_file_type("df") is False
    assert dl._should_download_file_type("ePub") is True
    assert dl._should_download_file_type("mobi") is False


def test_include_logic_empty():
    dl = DownloadLibrary(
        "fake_library_path",
        ext_include=[],
    )
    assert dl._should_download_file_type("pdf") is True
    assert dl._should_download_file_type("df") is True
    assert dl._should_download_file_type("EPub") is True
    assert dl._should_download_file_type("mobi") is True


def test_exclude_logic_has_values():
    dl = DownloadLibrary(
        "fake_library_path",
        ext_exclude=["pdf", "EPub"],
    )
    assert dl._should_download_file_type("pdf") is False
    assert dl._should_download_file_type("df") is True
    assert dl._should_download_file_type("ePub") is False
    assert dl._should_download_file_type("mobi") is True


def test_exclude_logic_empty():
    dl = DownloadLibrary(
        "fake_library_path",
        ext_exclude=[],
    )
    assert dl._should_download_file_type("pdf") is True
    assert dl._should_download_file_type("df") is True
    assert dl._should_download_file_type("EPub") is True
    assert dl._should_download_file_type("mobi") is True


###
# _should_download_platform
###
def test_download_platform_filter_none():
    dl = DownloadLibrary(
        "fake_library_path",
        platform_include=None,
    )
    assert dl._should_download_platform("ebook") is True
    assert dl._should_download_platform("audio") is True


def test_download_platform_filter_blank():
    dl = DownloadLibrary(
        "fake_library_path",
        platform_include=[],
    )
    assert dl._should_download_platform("ebook") is True
    assert dl._should_download_platform("audio") is True


def test_download_platform_filter_audio():
    dl = DownloadLibrary(
        "fake_library_path",
        platform_include=["audio"],
    )
    assert dl._should_download_platform("ebook") is False
    assert dl._should_download_platform("audio") is True


###
# timeout and retry configuration
###
def test_download_timeout_default():
    dl = DownloadLibrary("fake_library_path")
    assert dl.download_timeout == 30


def test_download_timeout_zero_means_none():
    dl = DownloadLibrary("fake_library_path", download_timeout=0)
    assert dl.download_timeout is None


def test_retry_count_default():
    dl = DownloadLibrary("fake_library_path")
    assert dl.retry_count == 3


def test_retry_count_custom():
    dl = DownloadLibrary("fake_library_path", retry_count=5)
    assert dl.retry_count == 5


###
# _download_file
###
def test_download_file_success(tmp_path):
    dl = DownloadLibrary("fake_library_path", download_timeout=10)
    local_file = str(tmp_path / "test_file.bin")
    test_data = b"hello world"

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"content-length": str(len(test_data))}
    mock_response.iter_content.return_value = [test_data]

    dl.session = MagicMock()
    dl.session.get.return_value = mock_response

    dl._download_file("http://example.com/file.bin", local_file)

    assert os.path.isfile(local_file)
    with open(local_file, "rb") as f:
        assert f.read() == test_data

    dl.session.get.assert_called_once_with(
        "http://example.com/file.bin",
        stream=True,
        timeout=10,
        headers={},
    )


def test_download_file_resume_with_206(tmp_path):
    dl = DownloadLibrary("fake_library_path", download_timeout=10)
    local_file = str(tmp_path / "test_file.bin")

    # Write partial content
    with open(local_file, "wb") as f:
        f.write(b"hello")

    remaining_data = b" world"
    mock_response = MagicMock()
    mock_response.status_code = 206
    mock_response.headers = {"content-length": str(len(remaining_data))}
    mock_response.iter_content.return_value = [remaining_data]

    dl.session = MagicMock()
    dl.session.get.return_value = mock_response

    dl._download_file("http://example.com/file.bin", local_file)

    with open(local_file, "rb") as f:
        assert f.read() == b"hello world"

    dl.session.get.assert_called_once_with(
        "http://example.com/file.bin",
        stream=True,
        timeout=10,
        headers={"Range": "bytes=5-"},
    )


def test_download_file_resume_server_ignores_range(tmp_path):
    dl = DownloadLibrary("fake_library_path", download_timeout=10)
    local_file = str(tmp_path / "test_file.bin")

    # Write partial content
    with open(local_file, "wb") as f:
        f.write(b"old partial data")

    full_data = b"complete new file"
    mock_response = MagicMock()
    mock_response.status_code = 200  # Server ignored Range header
    mock_response.headers = {"content-length": str(len(full_data))}
    mock_response.iter_content.return_value = [full_data]

    dl.session = MagicMock()
    dl.session.get.return_value = mock_response

    dl._download_file("http://example.com/file.bin", local_file)

    with open(local_file, "rb") as f:
        assert f.read() == full_data


def test_download_file_416_retries_without_range(tmp_path):
    dl = DownloadLibrary("fake_library_path", download_timeout=10)
    local_file = str(tmp_path / "test_file.bin")

    # Write partial content
    with open(local_file, "wb") as f:
        f.write(b"old data")

    full_data = b"new complete file"
    mock_416 = MagicMock()
    mock_416.status_code = 416

    mock_200 = MagicMock()
    mock_200.status_code = 200
    mock_200.headers = {"content-length": str(len(full_data))}
    mock_200.iter_content.return_value = [full_data]

    dl.session = MagicMock()
    dl.session.get.side_effect = [mock_416, mock_200]

    dl._download_file("http://example.com/file.bin", local_file)

    with open(local_file, "rb") as f:
        assert f.read() == full_data

    assert dl.session.get.call_count == 2


###
# _process_download retry logic
###
@patch("humblebundle_downloader.download_library.time.sleep")
def test_process_download_retries_on_failure(mock_sleep, tmp_path):
    dl = DownloadLibrary(
        str(tmp_path), download_timeout=10, retry_count=2
    )
    dl.cache_file = str(tmp_path / ".cache.json")
    dl.cache_data = {}

    with patch.object(dl, "_download_file") as mock_dl:
        mock_dl.side_effect = [
            ConnectionError("connection reset"),
            None,  # success on 2nd attempt
        ]
        result = dl._process_download(
            "http://example.com/file.bin",
            "cache_key",
            {"url_last_modified": "Thu, 01 Jan 2025 00:00:00 GMT"},
            str(tmp_path / "output.bin"),
        )

    assert result is True
    assert mock_dl.call_count == 2
    mock_sleep.assert_called_once_with(2)  # 2^1 = 2


@patch("humblebundle_downloader.download_library.time.sleep")
def test_process_download_all_retries_exhausted(mock_sleep, tmp_path):
    dl = DownloadLibrary(
        str(tmp_path), download_timeout=10, retry_count=2
    )
    dl.cache_file = str(tmp_path / ".cache.json")
    dl.cache_data = {}

    with patch.object(dl, "_download_file") as mock_dl:
        mock_dl.side_effect = ConnectionError("connection reset")
        result = dl._process_download(
            "http://example.com/file.bin",
            "cache_key",
            {},
            str(tmp_path / "output.bin"),
        )

    assert result is False
    assert mock_dl.call_count == 3  # 1 initial + 2 retries


@patch("humblebundle_downloader.download_library.time.sleep")
def test_process_download_no_retries(mock_sleep, tmp_path):
    dl = DownloadLibrary(
        str(tmp_path), download_timeout=10, retry_count=0
    )
    dl.cache_file = str(tmp_path / ".cache.json")
    dl.cache_data = {}

    with patch.object(dl, "_download_file") as mock_dl:
        mock_dl.side_effect = ConnectionError("connection reset")
        result = dl._process_download(
            "http://example.com/file.bin",
            "cache_key",
            {},
            str(tmp_path / "output.bin"),
        )

    assert result is False
    assert mock_dl.call_count == 1
    mock_sleep.assert_not_called()
