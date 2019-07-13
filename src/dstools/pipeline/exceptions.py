
class TaskBuildError(Exception):
    """Raise when a task fails to build
    """
    pass


class RenderError(Exception):
    """Raise when a template fails to render
    """
    pass
