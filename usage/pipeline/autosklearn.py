from dstools.pipeline import Pipeline
from dstools.util import config, load_yaml
from dstools.sklearn.util import model_name

import pandas as pd
import autosklearn.classification
from sklearn.preprocessing import StandardScaler


def load(config):
    data = {}
    # data loading
    train = pd.read_csv('../data/train.csv')
    data['train_x'] = train.drop('survived', axis=1).values
    data['train_y'] = train.survived.values
    test = pd.read_csv('../data/test.csv')
    data['test_x'] = test.values
    # scale data
    scaler = StandardScaler().fit(data['train_x'])
    data['train_x'] = scaler.transform(data['train_x'])
    data['test_x'] = scaler.transform(data['test_x'])
    return data


def model_gen(config, models):
    cls = autosklearn.classification.AutoSklearnClassifier()
    return [cls]


def train(config, model, data, record):
    model.fit(data['train_x'], data['train_y'])
    best = model.get_estimator()
    record['parameters'] = best.get_params()
    record['model'] = model_name(best)


def post_train(config, models, data, record):
    pass


def finalize(config, models, experiment):
    for r in experiment.records:
        print r.parameters

    # save results from this experiment
    experiment.save()

pip = Pipeline(config, load_yaml('exp.yaml'))

pip.load = load
pip.model_gen = model_gen
pip.train = train
# pip.post_train = post_train
pip.finalize = finalize

pip()
