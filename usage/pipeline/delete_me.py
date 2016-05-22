from dstools.pipeline import Pipeline
from dstools.config import main
from dstools.config import load as load_config
from dstools.lab.util import top_k
from dstools.sklearn import grid_generator
from dstools.sklearn.util import model_name

from sklearn.datasets import load_iris
from sklearn.metrics import precision_score
from sklearn.cross_validation import train_test_split

# define your custom functions


# this function should return the all the data used to train models
#  must return a dictionary. In subsequentent functions the data will
#  be available in the 'data' parameter
def load(config):
    print config
    iris = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(iris.data, iris.target,
                                                        test_size=0.30)
    data = {
        'X_train': X_train,
        'X_test': X_test,
        'y_train': y_train,
        'y_test': y_test
    }
    return data


# optional - once the data is loaded, this will ve called on each iteration
# to subselect features
def feature_selection(config, models, data, record):
    pass


# this function is called on every iteration, it must return an unfitted
# model
def model_gen(config, models):
    classes = ['sklearn.ensemble.RandomForestClassifier']
    models = grid_generator.grid_from_classes(classes)
    for m in models:
        yield m


# function used to train models, should return
# a fitted model
def train(config, model, data, record):
    print record

    model.fit(data['X_train'], data['y_train'])
    preds = model.predict(data['X_test'])

    record['precision'] = precision_score(data['y_test'], preds)
    record['parameters'] = model.get_params()
    record['model'] = model_name(model)
    return model


# optional function used when every model has been trained
def finalize(config, models, experiment):
    experiment.records = top_k(experiment.records, 'precision', 2)

# create pipeline object
pip = Pipeline(main, load_config('exp.yaml'))

# assign your functions
pip.load = load
pip.feature_selection = feature_selection
pip.model_gen = model_gen
pip.train = train
pip.finalize = finalize

# run pipeline
pip()
