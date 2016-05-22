# intended pipeline usage
# logging to a database and python logging
# are configured automatically using the decorators
from pipeline import Pipeline
import quality

# SHOULD THIS BE A CLASS THAT RECEIVES ONLY A CONFIG DICT?

# If I assume the data is of a certain type, the decorators
# can also check for logic - e.g. after the impute function
# is run, there should be no NAs. Maybe I should assume
# they are numpy arrays

# how to decide which variables to log? sufix? prefix?
# what if I want to change what to log from the CLI?

# right know model generator will only change model settings
# but what if I want to also change features, for example?
# since the experiment config states which features to use
# this may be confusing but it's worth the explore the possibility.
# Same thing applies to transformer and scaler

pip = Pipeline()

# comment - I can leave name,featureset
# or abstract it on a Dataset class
# which is better?
@pip.loader
# maybe optiomally add other decorators to check data quality
# or even experiment consitency - e.g. raise exception if there
# is another experiment named the same
@quality.nas_threshold(0.5)
def load_dataset(name, featureset):
    dataset = custom_method_for_loading(name, featureset)
    return dataset


# optional decorator - skipped if missing
@pip.imputation
def impute(dataset):
    dataset = impute(dataset)
    return dataset


# this function is applied to the training data
# and subsequently applied to testing and validation
# using training set results
# optional decorator - skipped if missing
@pip.scaler
def scale(dataset):
    if dataset.name == 'training':
        scaler = scaler.fit(dataset)
    else:
        scaler = load_scaler()

    return scaler.apply(dataset)


@pip.generator
# this should return an iterator of models
# to be fitted - this is necessary since
# we cannot know the next model in advance
# maybe the user will use bayesian optimization
# and the next hyparameters set depend on
# previous results
def model_generator():
    pass


# train, validate and test are the datasets
# model is an instance that needs to be trained
# this method fits a model to a training dataset,
# performs optimization using the validation set
# and calcualtes metrics using the test set.
# the function is run as many times as needed
@pip.model
def model(train, validate, test, model):
    pass


# this last step is optional, if present
# will let the user decide which models
# to log, if not presend @model should
# decide which models to log
@pip.finalize
def finalize(models):
    pass

