import uuid
from datetime import datetime
from enum import StrEnum
from time import time
from typing import Callable

from schemas import JobSchema
from storage import Storage


class JobStatusEnum(StrEnum):
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"


class Job:
    def __init__(self,
                 func: Callable,
                 start_at: datetime = datetime.now(),
                 max_working_time=-1, tries=1,
                 dependencies: list["Job"] = None,
                 args: tuple = None,
                 kwargs: dict = None,
                 task_id=None):
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.__coroutine = func(*self.args, **self.kwargs)
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies
        self.working_time = None
        self.status = JobStatusEnum.WAITING
        self.results = []
        self.task_id = task_id if task_id else uuid.uuid4()
        self.name = func.__name__

    def _run(self):
        result = self.__coroutine.send(None)
        self.results.append(result)

    def run(self):
        while self.tries > 0:
            try:
                if not self.working_time:
                    self.working_time = time()
                self.status = JobStatusEnum.RUNNING
                return self._run()
            except StopIteration:
                self.status = JobStatusEnum.COMPLETED
                raise StopIteration
            except Exception as e:
                self.tries -= 1

                if self.tries == 0:
                    self.status = JobStatusEnum.ERROR
                    raise e

    def pause(self):
        self.status = JobStatusEnum.PAUSED

    def stop(self):
        self.status = JobStatusEnum.STOPPED

    def resume(self):
        self.status = JobStatusEnum.WAITING

    @staticmethod
    def from_schema(data: JobSchema, storage: Storage) -> "Job":
        job = Job(
            task_id=data.task_id,
            func=storage.get_function(data.name),
            start_at=data.start_at,
            max_working_time=int(data.max_working_time),
            tries=data.tries,
            args=data.args,
            kwargs=data.kwargs
        )
        job.status = JobStatusEnum[data.status]
        job.dependencies = [Job.from_schema(dependency, storage)
                            for dependency in
                            data.dependencies] if data.dependencies else []
        return job
