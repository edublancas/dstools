from dstools.config import main
from dstools.lab import Experiment
from dstools.lab.util import group_by

ex = Experiment(main['logger'])
ex.get(['57410b666fdf1e2369805f79', '574111b16fdf1e28c7620981'])
model = ex.records[0]
model

#ex.save()

group_by(ex.records, 'model')
