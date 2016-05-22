from dstools.lab import Experiment


class Pipeline:
    def __init__(self, config):
        self.config = config
        self.models = []

    def _load(self):
        config = getattr(self.config, 'load', None)
        self.load(config, self.models)

    def _impute(self):
        pass

    def _scale(self):
        pass

    def _feature_selection(self, record):
        config = getattr(self.config, 'feature_selection', None)
        self.feature_selection(config, self.models, record)

    def _model_gen(self):
        return []

    def _train(self, model, record):
        pass

    def _finalize(self, experiment):
        pass

    def __call__(self):
        # create experiment instance
        ex = Experiment(self.conf['experiment'])

        self._load()

        if self._impute:
            self._impute()

        if self._scale():
            self._scale()

        # this can be parallelized
        # but if we are doing hyperparameter optimization
        # is not going to be trivial
        for model in self._model_gen():
            record = ex.record()
            record['conf'] = self.conf

            if self.feature_selection:
                # pass record instance so the user is able
                # to record whatever he wants
                self._feature_selection(record)

            self._train(model, record)

        # last step - send experiment for post-processing
        self._finalize(self.models, ex)
        # save results from this experiment
        ex.save()
