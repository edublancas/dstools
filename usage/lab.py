from dstools.config import main
from dstools.lab import Experiment
from dstools.lab.util import top_k
from dstools.sklearn import grid_generator
from dstools.sklearn.util import model_name

from sklearn.datasets import load_iris
from sklearn.metrics import precision_score
from sklearn.cross_validation import train_test_split

classes = ['sklearn.ensemble.RandomForestClassifier']
models = grid_generator.grid_from_classes(classes)

iris = load_iris()
X_train, X_test, y_train, y_test = train_test_split(iris.data, iris.target,
                                                    test_size=0.30)

# create a new experiment
ex = Experiment(main['logger'])

for m in models:
    # create a new record
    rec = ex.record()

    m.fit(X_train, y_train)
    preds = m.predict(X_test)
    rec['precision'] = precision_score(y_test, preds)
    rec['parameters'] = m.get_params()
    rec['model'] = model_name(m)


# select top_k
ex.records = top_k(ex.records, 'precision', 2)

# store records in the database
ex.save()
