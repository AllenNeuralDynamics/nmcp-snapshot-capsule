from pathlib import Path
from typing import Callable, TypeVar
import time


T = TypeVar("T")


def with_retries(
    func: Callable[[], T], *, attempts: int = 3, base_sleep: float = 0.5
) -> T:
    """
    Simple retry helper with exponential backoff.
    Retries on any exception; returns immediately for successful calls (even if None).
    """
    for i in range(1, attempts + 1):
        try:
            return func()
        except Exception:
            if i == attempts:
                raise
            # Backoff with jitter
            sleep = base_sleep * (2 ** (i - 1))
            time.sleep(sleep + (sleep * 0.1))


def write_text_file(path_like: Path | str, content: str, *, encoding: str = "utf-8") -> None:
    """
    Write text content to disk, ensuring the parent directory exists.
    """
    path = Path(path_like)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding=encoding)


def write_bytes_file(path_like: Path | str, content: bytes) -> None:
    """
    Write binary content to disk, ensuring the parent directory exists.
    """
    path = Path(path_like)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
