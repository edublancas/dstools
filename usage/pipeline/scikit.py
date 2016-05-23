from dstools.pipeline import Pipeline
from dstools.util import config, load_yaml
from dstools.sklearn.util import model_name
from dstools.sklearn import grid_generator
from dstools.lab.util import top_k

import pandas as pd
from sklearn import cross_validation
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import SelectPercentile, f_classif
from sklearn.pipeline import make_pipeline
from itertools import product
import logging

log = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.DEBUG)

'''
    Pipeline example using scikit-learn
    - Scaling features
    - Training different models and hyperparameter sets
    - Feature selection using SelectPercentile
'''


def load(config):
    data = {}
    # data loading
    train = pd.read_csv('../data/train.csv', index_col='id')
    data['train_x'] = train.drop('survived', axis=1).values
    data['train_y'] = train.survived.values
    test = pd.read_csv('../data/test.csv', index_col='id')
    data['test_x'] = test.values
    data['test_ids'] = test.index
    return data


# maybe this should be able to modify the data send to train
# for the next iteration - to avoid redundant transformations
def model_iterator(config):
    classes = ['sklearn.ensemble.RandomForestClassifier',
               'sklearn.ensemble.AdaBoostClassifier',
               'sklearn.linear_model.LogisticRegression',
               'sklearn.ensemble.ExtraTreesClassifier',
               'sklearn.ensemble.GradientBoostingClassifier']
    classes = ['sklearn.ensemble.ExtraTreesClassifier']
    sklearn_models = grid_generator.grid_from_classes(classes, size='small')
    percentiles = [50, 60, 70, 80, 90, 100]
    all_models = list(product(sklearn_models, percentiles))
    return all_models


def train(config, model, data, record):
    model, percentile = model

    try:
        model.n_jobs = -1
    except:
        log.info('Cannot set n_jobs for this model...')

    record['model'] = model_name(model)
    record['parameters'] = model.get_params()
    record['feats_percentile'] = percentile

    train_x = data['train_x']
    train_y = data['train_y']
    test_x = data['test_x']

    # estimate accuracy using cross-validation
    model = make_pipeline(SelectPercentile(f_classif, percentile),
                          StandardScaler(), model)

    scores = cross_validation.cross_val_score(model, train_x,
                                              train_y, cv=5,
                                              scoring='accuracy')
    record['mean_acc'] = scores.mean()

    # predict on the test set
    fn = SelectPercentile(f_classif, percentile).fit(train_x, train_y)
    train_x = fn.transform(train_x)
    test_x = fn.transform(test_x)

    scaler = StandardScaler().fit(train_x)
    train_x = scaler.transform(train_x)
    test_x = scaler.transform(test_x)

    model.fit(train_x, train_y)
    ids = data['test_ids']
    preds = model.predict(test_x)
    record['test_preds'] = [(id_, pred) for id_, pred in zip(ids, preds)]


def finalize(config, experiment):
    experiment.records = top_k(experiment.records, 'mean_acc', 10)
    experiment['exp_name'] = 'fixed-transform-2'
    experiment.save()

pip = Pipeline(config, load_yaml('exp.yaml'))

pip.load = load
pip.model_iterator = model_iterator
pip.train = train
pip.finalize = finalize

pip()
