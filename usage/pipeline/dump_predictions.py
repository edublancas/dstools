from dstools.util import load_yaml
from dstools.lab import Experiment
import pandas as pd

ex = Experiment(load_yaml('exp.yaml'))
ex.get(_id=['574292c66fdf1e3a36053b05'])
best = ex.records[0]

df = pd.DataFrame(best.test_preds, columns=['PassengerId', 'Survived'])
df.set_index('PassengerId', inplace=True)
df.Survived = df.Survived.astype(int)
df.to_csv('res.csv')
