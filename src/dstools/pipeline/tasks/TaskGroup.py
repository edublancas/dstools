from dstools.util import isiterable


class TaskGroup:
    """
    A collection of Tasks, used internally for enabling operador overloading

    (task1 + task2) >> task3
    """
    # TODO: implement mapping interface, task.upstream should return
    # a TaskGroup object, make ipython autocomplete work in getitem,
    # also add logic to verify if all keys were used, to detect tasks
    # where not all upstream dependencies are used. That means, they should
    # not be dependencies. Checking should be implemented in getitem,
    # but possibly in pop, popitem

    def __init__(self, tasks):
        self.tasks = tasks

    def __iter__(self):
        for t in self.tasks:
            yield t

    def __add__(self, other):
        if isiterable(other):
            return TaskGroup(list(self.tasks) + list(other))
        else:
            return TaskGroup(list(self.tasks) + [other])

    def __radd__(self, other):
        if isiterable(other):
            return TaskGroup(list(other) + list(self.tasks))
        else:
            return TaskGroup([other] + list(self.tasks))

    def set_upstream(self, other):
        if isiterable(other):
            for t in self.tasks:
                for o in other:
                    t.set_upstream(other)
        else:
            for t in self.tasks:
                t.set_upstream(other)

    # FIXME: implement render

    def __rshift__(self, other):
        other.set_upstream(self)
        # return other so a >> b >> c works
        return other
