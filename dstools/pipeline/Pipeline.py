from dstools.lab import Experiment
import logging

log = logging.getLogger(__name__)

MAX_WORKERS = 20


class Pipeline:
    def __init__(self, config, exp_config):
        self.config = config
        # initialize functions as None
        self.load = None
        self.model_gen = None
        self.train = None
        self.post_train = None
        self.finalize = None
        # create experiment instance
        self.ex = Experiment(**exp_config)
        self.models = []

    def _load(self):
        config = getattr(self.config, 'load', None)
        self.data = self.load(config)

    def _model_gen(self):
        config = getattr(self.config, 'model_gen', None)
        return self.model_gen(config, self.models)

    def _train(self, model, record):
        config = getattr(self.config, 'train', None)
        return self.train(config, model, self.data, record)

    def _post_train(self, record):
        config = getattr(self.config, 'post_train', None)
        self.post_train(config, self.models, self.data, record)

    def _finalize(self, experiment):
        config = getattr(self.config, 'finalize', None)
        return self.finalize(config, self.models, experiment)

    def __call__(self):
        log.info('Pipeline started. Loading data.')
        self._load()
        log.info('Data loaded. Starting models loop.')

        model_gen = self._model_gen()
        # see if self._model_gen has len
        try:
            total = len(model_gen)
        except:
            log.info('Number of models to train is unknown')
            total = None
        else:
            log.info('Models to train: {}'.format(total))

        #self._serial_run(model_gen, total)
        self._concurrent_run(model_gen, total)

        #log.info('Running finalize step.')
        #self._finalize(self.ex)

    def _serial_run(self, model_gen, total):
        for i, model in enumerate(model_gen, 1):
            self._one_step(model, i, total)

    def _concurrent_run(self, model_gen, total):
        from concurrent import futures
        with futures.ThreadPoolExecutor(MAX_WORKERS) as executor:
            executor.map(self._one_step, model_gen, range(total),
                         [total]*total)

    def _one_step(self, model, i, total):
        if total:
            log.info('{}/{} - Running with: {}'.format(i, total, model))
        else:
            log.info('{} - Running with: {}'.format(i, model))

        record = self.ex.record()
        record['config'] = self.config

        # run training functiona and save fitted
        # model in self.models
        res = self._train(model, record)
        self.models.append(res)

        if self.post_train:
            log.info('Post train.')
            self._post_train(record)
