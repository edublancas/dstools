from dstools.util import load_yaml
from dstools.lab import Experiment
import pandas as pd

ex = Experiment(load_yaml('exp.yaml')['conf'])
ex.get(_id=['57435574e0f48c88752991ba'])
best = ex.records[0]

df = pd.DataFrame(best.test_preds, columns=['PassengerId', 'Survived'])
df.set_index('PassengerId', inplace=True)
df.Survived = df.Survived.astype(int)
df.to_csv('res.csv')
