"""Shared fakes for the offline Silver job tests (no PySpark)."""

from __future__ import annotations

from typing import Any


class FakeQuery:
    def __init__(self, error: Any = None, await_raises: Exception | None = None) -> None:
        self.id = "qid-1"
        self.isActive = True
        self.stopped = False
        self._error = error
        self._await_raises = await_raises
        self.awaited = False

    def awaitTermination(self) -> None:
        self.awaited = True
        if self._await_raises:
            raise self._await_raises

    def exception(self) -> Any:
        return self._error

    def stop(self) -> None:
        self.stopped = True
        self.isActive = False


class Chain:
    """Records every call of a fluent ``writeStream`` chain; ``foreachBatch`` keeps the callback."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.callback: Any = None

    def __getattr__(self, name: str) -> Any:
        def method(*args: Any, **kw: Any) -> Any:
            self.calls.append((name, args, kw))
            if name == "foreachBatch":
                self.callback = args[0]
            if name == "start":
                return FakeQuery()
            return self

        return method


class Reader:
    def __init__(self, stream: Chain) -> None:
        self.stream = stream
        self.loads: list[str] = []
        self.options: list[tuple[str, Any]] = []
        self.formats: list[str] = []

    def format(self, fmt: str) -> Reader:
        self.formats.append(fmt)
        return self

    def option(self, k: str, v: Any) -> Reader:
        self.options.append((k, v))
        return self

    def load(self, path: str) -> Any:
        self.loads.append(path)
        return type("Src", (), {"writeStream": self.stream, "path": path})()


class FakeSpark:
    def __init__(self, chain: Chain) -> None:
        self.read = Reader(chain)
        self.readStream = Reader(chain)
