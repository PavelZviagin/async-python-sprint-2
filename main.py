from job import Job
from scheduler import Scheduler
from tasks import (create_and_write, create_directory, delete_directory,
                   delete_file, print_something, storage)

if __name__ == '__main__':
    job1 = Job(func=print_something, args=(1,))
    job2 = Job(func=create_directory, args=('test',), tries=3)
    job3 = Job(func=create_and_write,
               args=('test/test.txt', 'Hello world!'),
               dependencies=[job2])
    job4 = Job(func=delete_file, args=('test/test.txt',), dependencies=[job3])
    job5 = Job(func=delete_directory, args=('test',), dependencies=[job4])
    schedule = Scheduler(storage)
    schedule.schedule(job1)
    schedule.schedule(job5)
    schedule.start()
    schedule.join()
