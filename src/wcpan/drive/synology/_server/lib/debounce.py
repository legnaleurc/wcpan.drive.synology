import asyncio
from collections.abc import Callable, Coroutine
from typing import Any


type AsyncFactory = Callable[[], Coroutine[Any, Any, None]]
type DebounceErrorHandler = Callable[[str, Exception], None]
type DebounceDoneHandler = Callable[[asyncio.Task[None]], None]


class Debouncer:
    """Runs one delayed task without canceling it after the body starts."""

    def __init__(
        self,
        group: asyncio.TaskGroup,
        delay: float,
        *,
        key: str = "",
        on_error: DebounceErrorHandler | None = None,
        on_done: DebounceDoneHandler | None = None,
    ) -> None:
        self._group = group
        self._delay = delay
        self._key = key
        self._on_error = on_error
        self._on_done = on_done
        self._task: asyncio.Task[None] | None = None
        self._factory: AsyncFactory | None = None
        self.started = False

    def start(self, factory: AsyncFactory) -> asyncio.Task[None]:
        if self._task is not None and not self._task.done():
            if self.started:
                return self._task
            self._task.cancel()

        self._factory = factory
        self.started = False
        task = self._group.create_task(self._run())
        self._task = task
        task.add_done_callback(self._done)
        return task

    def cancel_pending(self) -> bool:
        if self.started or self._task is None:
            return False
        self._task.cancel()
        return True

    def is_task(self, task: asyncio.Task[None]) -> bool:
        return self._task is task

    def _done(self, task: asyncio.Task[None]) -> None:
        if self._on_done is not None:
            self._on_done(task)
        if self._task is task:
            self._task = None
            self._factory = None
            self.started = False

    async def _run(self) -> None:
        await asyncio.sleep(self._delay)
        self.started = True
        if self._factory is None:
            return
        try:
            await self._factory()
        except Exception as e:
            if self._on_error is None:
                raise
            self._on_error(self._key, e)


class TaskIdDebouncer:
    """Debounces work independently for each task id."""

    def __init__(
        self,
        group: asyncio.TaskGroup,
        delay: float,
        *,
        on_error: DebounceErrorHandler | None = None,
    ) -> None:
        self._group = group
        self._delay = delay
        self._on_error = on_error
        self._debouncers: dict[str, Debouncer] = {}

    def schedule(
        self,
        task_id: str,
        factory: AsyncFactory,
    ) -> None:
        debouncer = self._debouncers.get(task_id)
        if debouncer is None:

            def cleanup(done: asyncio.Task[None]) -> None:
                current = self._debouncers.get(task_id)
                if current is not None and current.is_task(done):
                    self._debouncers.pop(task_id, None)

            debouncer = Debouncer(
                self._group,
                self._delay,
                key=task_id,
                on_error=self._on_error,
                on_done=cleanup,
            )
            self._debouncers[task_id] = debouncer
        debouncer.start(factory)

    def cancel(self, task_id: str) -> None:
        if (current := self._debouncers.get(task_id)) and not current.started:
            self._debouncers.pop(task_id, None)
            current.cancel_pending()
