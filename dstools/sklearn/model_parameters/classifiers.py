#Ensemble methods
AdaBoostClassifier = {
    # Ignoring "base_estimator" for now
    "n_estimators": {"type": int, "default": 50},
    "learning_rate": {"type": float, "default": 1.},
    "algorithm": ["SAMME", "SAMME.R"],
    #Ignoring random_state for now
}

RandomForestClassifier = {
    "n_estimators": {"type": int, "default": 10},
    "criterion": ["gini", "entropy"],
    #Ignoring many parameters for now...
    "bootstrap": {"type": bool},
}

#Support Vector Machines
SVC = {
    "C": {"type": float, "default": 1.0},
    "kernel": ["linear", "poly", "rbf", "sigmoid", "precomputed"],

}