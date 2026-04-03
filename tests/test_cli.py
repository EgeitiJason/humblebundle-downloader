import pytest
from humblebundle_downloader.cli import parse_args


def test_old_action_format():
    with pytest.raises(DeprecationWarning):
        _ = parse_args(["download", "-l", "some_path", "-c", "fake_cookie"])


def test_no_action():
    args = parse_args(["-l", "some_path", "-c", "fake_cookie"])
    assert args.library_path == "some_path"
    assert args.cookie_file == "fake_cookie"


def test_no_args():
    with pytest.raises(SystemExit) as ex:
        parse_args([])
    assert ex.value.code == 2


def test_download_timeout_default():
    args = parse_args(["-l", "some_path", "-c", "fake_cookie"])
    assert args.download_timeout == 30


def test_download_timeout_custom():
    args = parse_args(
        ["-l", "some_path", "-c", "fake_cookie", "--download-timeout", "60"]
    )
    assert args.download_timeout == 60


def test_retry_count_default():
    args = parse_args(["-l", "some_path", "-c", "fake_cookie"])
    assert args.retry_count == 3


def test_retry_count_custom():
    args = parse_args(
        ["-l", "some_path", "-c", "fake_cookie", "-r", "5"]
    )
    assert args.retry_count == 5
