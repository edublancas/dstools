from tabulate import tabulate


class BuildStatus:
    """A class to encapsulate the status of a task/dag after building
    """

    def __init__(self, run, elapsed):
        self.run = run
        self.elapsed = elapsed
        self.components = None

    @classmethod
    def from_components(cls, components):
        names = [t.name for t in components.keys()]
        status = components.values()
        total = sum([s.elapsed or 0 for s in status])

        def prop(elapsed, total):
            if elapsed is None:
                return None
            else:
                return 100 * elapsed / total

        rows = [(n, s.run, s.elapsed, prop(s.elapsed, total))
                for n, s in zip(names, status)]

        run = any(s.run for s in status)
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
