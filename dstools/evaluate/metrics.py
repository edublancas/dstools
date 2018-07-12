
def classification_metrics(metrics, y_true, y_pred):
    """Evaluate several metrics, returns a dictionary with
    {metric_name: metric_value} pairs
    """
    return {metric.__name__: metric(y_true=y_true, y_pred=y_pred)
            for metric in metrics}
