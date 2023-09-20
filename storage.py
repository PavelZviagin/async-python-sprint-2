def to_storage(storage):
    def decorator(fn):
        storage.append_function(fn.__name__, fn)
        return fn

    return decorator


class Storage:
    def __init__(self):
        self._storage = {}

    def append_function(self, name, fn):
        if name not in self._storage:
            self._storage[name] = fn

    def get_function(self, name):
        if name in self._storage:
            return self._storage[name]

        raise ValueError(f'Function {name} not found')
