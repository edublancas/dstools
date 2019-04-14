_TASKS = []
_NON_END_TASKS = []


def build_all():
    for task in _TASKS:
        task._already_checked = False

        if task.is_end_task:
            task.build()
