import os
from functools import wraps
from time import sleep

from storage import Storage, to_storage

storage = Storage()


def coroutine(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen

    return wrap


@to_storage(storage)
@coroutine
def create_directory(path):
    _ = yield
    yield os.mkdir(path)


@to_storage(storage)
@coroutine
def delete_directory(path):
    _ = yield
    yield os.rmdir(path)


@to_storage(storage)
@coroutine
def create_and_write(path, content):
    _ = yield
    with open(path, 'w') as f:
        yield f.write(content)


@to_storage(storage)
@coroutine
def read_file(path):
    _ = yield
    with open(path, 'r') as f:
        yield f.readlines()


@to_storage(storage)
@coroutine
def delete_file(path):
    _ = yield
    yield os.remove(path)


@to_storage(storage)
@coroutine
def get_request(url):
    import requests
    _ = yield
    yield requests.get(url).text


def count_spaces(text):
    return text.count(' ')


@to_storage(storage)
@coroutine
def text_count_spaces(text):
    _ = yield
    yield count_spaces(text)


@to_storage(storage)
@coroutine
def print_something(_id):
    _ = yield
    sleep(1)
    yield print(_id)
