_TASKS = []
_NON_END_TASKS = []


def build_all():
    for task in _TASKS:
        if task.is_end_task:
            task.build()
