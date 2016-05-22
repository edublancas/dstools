from dstools.config import main
from dstools.lab import Experiment
from dstools.lab.util import group_by

ex = Experiment(main['logger'])
ex.get(['574107f46fdf1e21cea90844'])
model = ex.records[0]

model['key'] = 'new value'

new_model = ex.record()
new_model['key'] = 'im super cool'

ex.save()

group_by(ex.records, 'model')
