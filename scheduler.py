import datetime
from queue import Queue
from threading import Thread
from time import time

from loguru import logger

from job import Job, JobStatusEnum
from schemas import JobListSchema
from storage import Storage


class Scheduler(Thread):
    def __init__(self, storage: Storage, pool_size: int = 10):
        super().__init__()
        self.pool_size = pool_size
        self.jobs = []
        self.__queue = Queue()
        self.is_stopped = False
        self._storage = storage

    def schedule(self, task: Job):
        if task.status not in [JobStatusEnum.WAITING, JobStatusEnum.RUNNING]:
            return

        job_ids = [job.id for job in self.jobs]

        if task.id in job_ids:
            return

        if self._check_dependency_error(task):
            task.status = JobStatusEnum.ERROR
            return

        if len(self.jobs) < self.pool_size:
            if task.dependencies:
                for dependency in task.dependencies:
                    if dependency not in self.jobs:
                        self.schedule(dependency)
            self.jobs.append(task)
        else:
            self.__queue.put(task)

    @staticmethod
    def _check_dependency_error(task: Job) -> bool:
        if task.dependencies:
            return any(dependency.status == JobStatusEnum.ERROR
                       for dependency in task.dependencies)

        return False

    def run(self):
        while self.jobs and not self.is_stopped:
            task = self.jobs.pop(0)

            if not self._check_dependency(task):
                logger.warning(f"Task dependencies are not completed: {task.name}")
                self.schedule(task)
                continue

            if not self._check_working_time(task):
                logger.warning(f"Task working time is over {task.name}")
                task.status = JobStatusEnum.ERROR
                continue

            if not self._check_start_time(task):
                logger.warning(f"Task start time is not come: {task.name}")
                self.schedule(task)
                continue

            try:
                logger.debug(f"Task started: {task.name}")
                task.run()
            except StopIteration:
                self._check_and_append_task_from_queue()
                continue
            except Exception:  # noqa
                continue

            self.schedule(task)

    @staticmethod
    def _check_start_time(task: Job) -> bool:
        if task.start_at > datetime.datetime.now():
            return False

        return True

    @staticmethod
    def _check_working_time(task: Job) -> bool:
        if task.max_working_time == -1 or not task.working_time:
            return True

        if time() - task.working_time > task.max_working_time:
            return False

        return True

    @staticmethod
    def _check_dependency(task: Job) -> bool:
        if task.dependencies:
            return all(dependency.status == JobStatusEnum.COMPLETED
                       for dependency in task.dependencies)

        return True

    def _check_and_append_task_from_queue(self):
        if len(self.jobs) < self.pool_size:
            if not self.__queue.empty():
                task = self.__queue.get()
                self.jobs.append(task)

    def restart(self):
        self.is_stopped = False

        schema: JobListSchema = self._load_schema_from_file()
        for job in schema.jobs:
            new_job = Job.from_schema(job, self._storage)
            self.schedule(new_job)

        self.start()

    @staticmethod
    def _load_schema_from_file(file_name: str = 'data.json') -> JobListSchema:
        with open(file_name, 'r') as file:
            schema = JobListSchema.model_validate_json(file.read())
        return schema

    @staticmethod
    def _save_schema_to_file(schema: JobListSchema, file_name: str = 'data.json'):
        with open(file_name, 'w') as file:
            file.write(schema.model_dump_json())

    def stop(self):
        self.is_stopped = True

        while not self.__queue.empty():
            self.jobs.append(self.__queue.get())

        job_list = []
        for job in self.jobs:
            job_dict = job.__dict__
            if job.dependencies:
                job_dict['dependencies'] = [dependency.__dict__
                                            for dependency in job.dependencies]
            job_list.append(job_dict)

        schema = JobListSchema(jobs=job_list)
        self._save_schema_to_file(schema)
        self.jobs.clear()
        logger.warning('Scheduler stopped')
