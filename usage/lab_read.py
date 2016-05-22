from dstools.config import main
from dstools.lab import Experiment
from dstools.lab.util import group_by

ex = Experiment(main['logger'])
ex.get(_id=['574107f46fdf1e21cea90844'])
ex.get(key='im super cool')
ex.records

model = ex.records[0]
model['key'] = 'new value2'

new_model = ex.record()
new_model['key'] = 'im super cool'

ex.save()

group_by(ex.records, 'model')
