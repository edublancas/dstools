from experiment import Experiment
from dstools.sklearn import grid_generator
import pandas as pd

class SklearnExp(Experiment):
    def load(self):
        train_data = pd.read_csv('data/train.csv')
        self.training_x = train_data.drop('survived', axis=1).values
        self.training_y = train_data.survived.values

    def model_gen(self):
        models = grid_generator.grid_from_classes(['sklearn.ensemble.RandomForestClassifier'])
        for model in models:
            yield model

    def train(self, model):
        model = model.fit(self.training_x, self.training_y)
        self.models.append(model)

    def finalize(self):
        print self.models

exp = SklearnExp()
exp()
