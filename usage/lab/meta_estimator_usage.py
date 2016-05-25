from dstools.util import config
from dstools.lab import SKExperiment
from sklearn.datasets import load_iris
from sklearn.cross_validation import train_test_split
from sklearn.metrics import precision_score

# load skexperiment
ex = SKExperiment(config['logger'])

# get the record using its mongodb id
ex.get(_id=['5744fe7a6fdf1e49fdc22ff0'])

# take the first record
rec = ex.records[0]

# since we are using SKExperiment, records have a model
# attribute which returns a sklearn-like model,
# if we try to train the model, it will check if its the same
# data that it was originally trained on (using sha1 hashing)
# The wrapper only overrides fit and fir_transform methods,
# the rest of the attributes are passed to the original sklearn model
model = rec.model

# lets load the data
iris = load_iris()

# split the data using the same parameters that with DIFFERENT
# parameters
X_train, X_test, y_train, y_test = train_test_split(iris.data, iris.target,
                                                    test_size=0.30,
                                                    random_state=1)


# if we try to train the model, it will raise an exception
# since it's not the same data
model.fit(X_train, y_train)

# split the data using the same parameters that with the SAME
# parameters
X_train, X_test, y_train, y_test = train_test_split(iris.data, iris.target,
                                                    test_size=0.30,
                                                    random_state=0)

# this will work now
model.fit(X_train, y_train)

# at this point model contains
preds = model.predict(X_test)
precision_score(y_test, preds)

# if we want to get the sklearn model (without the wrapper)
model.skmodel
