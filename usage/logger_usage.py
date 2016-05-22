from dstools import ExperimentLogger
from dstools.sklearn import grid_generator

from experiment import Experiment
import pandas as pd

# create logger instance
exp_logger = ExperimentLogger()

train_data = pd.read_csv('data/train.csv')
training_x = train_data.drop('survived', axis=1).values
training_y = train_data.survived.values

models = grid_generator.grid_from_classes(['sklearn.ensemble.RandomForestClassifier'])

# ugly mode
for model in models:
    model = model.fit(training_x, training_y)

    rec = exp_logger.new_record()
    rec.add(some_metric)
    rec.add(model_name)
    rec.add(some_other_metric)
    rec.add(feature_set)
    rec.add(model_parameters)

# cool kids mode
# using the decorator will log everything that ends with '_log'
# note that using this mode restricts us to save records only from one function
# what if I want to save one record but the data is splitted among several
# functions?
@exp_logger.record_with_sufix('_log')
def train_model(model):
    model_log = model.fit(training_x, training_y)

    some_metric_log = #compute
    model_name_log = #compute
    some_other_metric_log = #compute
    feature_set_log = #compute
    model_parameters_log = #compute

for model in models:
    train_model(model)

# not so ugly mode
for model in models:
    model = model.fit(training_x, training_y)

    rec = exp_logger.new_record()
    rec.add(a_key=some_metric)
    rec.add(second_key=model_name)
    rec.add(metric_name=some_other_metric)
    rec.add(features=feature_set, params=model_parameters)


# since you may have logged a lot of records,
# decide which records save to the database
exp_logger.keep_n(lambda r: r['my_metric'], 20)

# save to the database
exp_logger.save()
