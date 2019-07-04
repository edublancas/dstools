from tabulate import tabulate

# TODO: separate this intro TaskBuildReport and DAGBuildReport


class BuildReport:
    """A class to report information after a task is built
    """

    def __init__(self, run, elapsed):
        """Create a build report from a single task
        """
        self.run = run
        self.elapsed = elapsed
        self.components = None

    @classmethod
    def from_components(cls, components):
        """Create a build report from several tasks
        """
        names = [t.name for t in components.keys()]
        report = components.values()
        total = sum([s.elapsed or 0 for s in report])

        def prop(elapsed, total):
            if elapsed is None:
                return None
            else:
                return 100 * elapsed / total

        rows = [(n, s.run, s.elapsed, prop(s.elapsed, total))
                for n, s in zip(names, report)]

        run = any(s.run for s in report)
        elapsed = total if run else None

        obj = cls(run=run, elapsed=elapsed)
        obj.components = components

        obj._table = tabulate(rows,
                              headers=['Name', 'Ran?',
                                       'Elapsed (s)', 'Percentage'],
                              floatfmt='.2f')

        return obj

    def __str__(self):
        s = f'Run: {self.run}, Elapsed: {self.elapsed}'

        if self.components:
            s = f'{self._table}\n'+s

        return s
