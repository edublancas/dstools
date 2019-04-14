_TASKS = []
_NON_END_TASKS = []


def build_all():
    # TODO: implement topological sorting to avoid this reverse order
    # execution, remove the _already_checked flag and the _NON_END_TASKS
    for task in _TASKS:
        task._already_checked = False

        if task.is_end_task:
            task.build()
