RandomForestClassifier = {
  'n_estimators': [1,10,100,1000,10000],
  'max_depth': [1,5,10,20,50,100],
  'max_features': ['sqrt','log2'],
  'min_samples_split': [2,5,10],
}

AdaBoostClassifier = {
  'algorithm': ['SAMME', 'SAMME.R'],
  'n_estimators': [1,10,100,1000,10000],
}

LogisticRegression = {
  'penalty': ['l1','l2'],
  'C': [0.00001,0.0001,0.001,0.01,0.1,1,10]
}

SVC = {
  'C': [0.00001,0.0001,0.001,0.01,0.1,1,10],
  'kernel': ['linear'],
}