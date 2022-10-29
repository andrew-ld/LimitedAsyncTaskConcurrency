import asyncio
import logging
import typing


class LimitedAsyncTaskConcurrency:
    __slots__ = ("_parallelism", "_tasks_queue", "_tasks", "_consumer")

    _tasks: dict[asyncio.Task, asyncio.Future]
    _tasks_queue: asyncio.Queue[tuple[typing.Callable, asyncio.Future, tuple[...]]]
    _parallelism: int
    _consumer: asyncio.Task

    def __init__(self, parallelism: int, limit_queue: int | None = None):
        self._parallelism = parallelism
        self._tasks_queue = asyncio.Queue() if limit_queue is None else asyncio.Queue(limit_queue)
        self._tasks = dict()
        self._consumer = asyncio.create_task(self._consume())

    async def _consume(self):
        get_from_queue_task = asyncio.create_task(self._tasks_queue.get())

        while True:
            has_get_from_queue_task = len(self._tasks) < self._parallelism

            if has_get_from_queue_task:
                completed_tasks, _ = await asyncio.wait((get_from_queue_task,) | self._tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
            else:
                completed_tasks, _ = await asyncio.wait(self._tasks.keys(), return_when=asyncio.FIRST_COMPLETED)

            if has_get_from_queue_task and get_from_queue_task in completed_tasks:
                completed_tasks.remove(get_from_queue_task)

                new_task_function, new_task_sentinel, new_task_arguments = get_from_queue_task.result()
                new_task_coroutine = asyncio.create_task(new_task_function(*new_task_arguments))
                self._tasks[new_task_coroutine] = new_task_sentinel

                get_from_queue_task = asyncio.create_task(self._tasks_queue.get())

            for completed_task in completed_tasks:
                completed_task_sentinel = self._tasks.pop(completed_task)

                if completed_task_exception := completed_task.exception():
                    completed_task_sentinel.set_exception(completed_task_exception)
                else:
                    completed_task_sentinel.set_result(completed_task.result())

    async def add_task(self, function: typing.Callable, *arguments: typing.Any) -> asyncio.Future:
        sentinel = asyncio.get_running_loop().create_future()
        await self._tasks_queue.put((function, sentinel, arguments))
        return sentinel

    def __del__(self):
        self._consumer.cancel()
