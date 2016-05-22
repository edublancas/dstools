from dstools.config import main
from dstools.lab import Experiment
from dstools.lab.util import group_by, group_map, group_reduce

ex = Experiment(main['logger'])
ex.get(name=['RandomForestClassifier', 'SVC'])
len(ex.records)

groups = group_by(ex.records, 'name')
maps = group_map(groups, lambda e:  e.name)
maps

group_reduce(maps, lambda x, y:  x+y)
