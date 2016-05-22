from dstools.lab import Experiment


class Pipeline:
    def __init__(self, config, exp_config):
        self.config = config
        # create experiment instance
        self.ex = Experiment(exp_config)
        self.models = []

    def _load(self):
        config = getattr(self.config, 'load', None)
        self.data = self.load(config)

    def _impute(self):
        pass

    def _scale(self):
        pass

    def _feature_selection(self, record):
        config = getattr(self.config, 'feature_selection', None)
        self.feature_selection(config, self.models, self.data, record)

    def _model_gen(self):
        config = getattr(self.config, 'model_gen', None)
        return self.model_gen(config, self.models)

    def _train(self, model, record):
        config = getattr(self.config, 'train', None)
        return self.train(config, model, self.data, record)

    def _finalize(self, experiment):
        config = getattr(self.config, 'finalize', None)
        return self.finalize(config, self.models, experiment)

    def __call__(self):
        self._load()

        if self._impute:
            self._impute()

        if self._scale():
            self._scale()

        # this can be parallelized
        # but if we are doing hyperparameter optimization
        # is not going to be trivial
        for model in self._model_gen():
            record = self.ex.record()
            record['config'] = self.config

            if self.feature_selection:
                # pass record instance so the user is able
                # to record whatever he wants
                self._feature_selection(record)

            # run training functiona and save fitted
            # model in self.models
            res = self._train(model, record)
            self.models.append(res)

        # last step - send experiment for post-processing
        self._finalize(self.ex)
        # save results from this experiment
        self.ex.save()
