from dstools.pipeline import Pipeline
from dstools.util import config, load_yaml
from dstools.sklearn.util import model_name
from dstools.sklearn import grid_generator
from dstools.lab.util import top_k

import pandas as pd
from sklearn import cross_validation
from sklearn.preprocessing import StandardScaler


def load(config):
    data = {}
    # data loading
    train = pd.read_csv('../data/train.csv', index_col='id')
    data['train_x'] = train.drop('survived', axis=1).values
    data['train_y'] = train.survived.values
    test = pd.read_csv('../data/test.csv', index_col='id')
    data['test_x'] = test.values
    # scale data
    scaler = StandardScaler().fit(data['train_x'])
    data['train_x'] = scaler.transform(data['train_x'])
    data['test_x'] = scaler.transform(data['test_x'])
    data['test_ids'] = test.index
    return data


def model_gen(config, models):
    classes = ['sklearn.ensemble.RandomForestClassifier',
               'sklearn.ensemble.AdaBoostClassifier',
               'sklearn.linear_model.LogisticRegression',
               'sklearn.ensemble.ExtraTreesClassifier',
               'sklearn.ensemble.GradientBoostingClassifier']
    classes = ['sklearn.ensemble.ExtraTreesClassifier']
    models = grid_generator.grid_from_classes(classes, size='small')
    for m in models:
        yield m


def train(config, model, data, record):
    scores = cross_validation.cross_val_score(model, data['train_x'],
                                              data['train_y'], cv=3,
                                              scoring='accuracy')
    model.fit(data['train_x'], data['train_y'])
    record['parameters'] = model.get_params()
    record['model'] = model_name(model)
    record['mean_acc'] = scores.mean()

    ids = data['test_ids']
    preds = model.predict(data['test_x'])
    record['test_preds'] = [(id_, pred) for id_, pred in zip(ids, preds)]


def post_train(config, models, data, record):
    pass


def finalize(config, models, experiment):
    experiment.records = top_k(experiment.records, 'mean_acc', 10)
    experiment['exp_name'] = 'testing'
    experiment.save()

pip = Pipeline(config, load_yaml('exp.yaml'))

pip.load = load
pip.model_gen = model_gen
pip.train = train
# pip.post_train = post_train
pip.finalize = finalize

pip()
