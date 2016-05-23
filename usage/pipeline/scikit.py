from dstools.pipeline import Pipeline
from dstools.util import config, load_yaml
from dstools.sklearn.util import model_name
from dstools.sklearn import grid_generator
from dstools.lab.util import top_k

import pandas as pd
from sklearn import cross_validation
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import SelectPercentile, f_classif
from itertools import product

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
    # scale data
    scaler = StandardScaler().fit(data['train_x'])
    data['train_x'] = scaler.transform(data['train_x'])
    data['test_x'] = scaler.transform(data['test_x'])
    data['test_ids'] = test.index
    return data


# maybe this should be able to modify the data send to train
# for the next iteration - to avoid redundant transformations
def model_gen(config, models):
    classes = ['sklearn.ensemble.RandomForestClassifier',
               'sklearn.ensemble.AdaBoostClassifier',
               'sklearn.linear_model.LogisticRegression',
               'sklearn.ensemble.ExtraTreesClassifier',
               'sklearn.ensemble.GradientBoostingClassifier']
    sklearn_models = grid_generator.grid_from_classes(classes, size='medium')
    percentiles = [50, 60, 70, 80, 90, 100]
    all_models = product(sklearn_models, percentiles)
    for m in all_models:
        yield m


def train(config, model, data, record):
    model, percentile = model

    train_x = data['train_x']
    train_y = data['train_y']
    test_x = data['test_x']

    fn = SelectPercentile(f_classif, percentile).fit(train_x, train_y)
    train_x = fn.transform(train_x)
    test_x = fn.transform(test_x)

    scores = cross_validation.cross_val_score(model, train_x,
                                              train_y, cv=3,
                                              scoring='accuracy')
    model.fit(train_x, train_y)
    record['parameters'] = model.get_params()
    record['model'] = model_name(model)
    record['mean_acc'] = scores.mean()
    record['feats_percentile'] = percentile

    ids = data['test_ids']
    preds = model.predict(test_x)
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
