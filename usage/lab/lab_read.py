from dstools.util import config
from dstools.lab import Experiment
from dstools.lab.util import group_by

ex = Experiment(config['logger'])
ex.get(_id=['5744d47f6fdf1e2f69f0716a'])
ex.get(key='im super cool')
ex.records

model = ex.records[0]
model['key'] = 'new value2'

new_model = ex.record()
new_model['key'] = 'im super cool'

ex.save()

group_by(ex.records, 'model')
