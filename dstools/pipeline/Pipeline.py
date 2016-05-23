from dstools.lab import Experiment
import logging

log = logging.getLogger(__name__)
MAX_WORKERS = 20


class Pipeline:
    def __init__(self, config, exp_config, workers=1):
        if workers > MAX_WORKERS:
            self._workers = MAX_WORKERS
            log.info('Max workers is {}.'.format(MAX_WORKERS))
        else:
            self._workers = workers

        self.config = config
        # initialize functions as None
        self.load = None
        self.model_iterator = None
        self.train = None
        self.finalize = None
        # create experiment instance
        self.ex = Experiment(**exp_config)

    def _load(self):
        config = getattr(self.config, 'load', None)
        self.data = self.load(config)

    def _model_iterator(self):
        config = getattr(self.config, 'model_iterator', None)
        return self.model_iterator(config)

    def _train(self, model, record):
        config = getattr(self.config, 'train', None)
        return self.train(config, model, self.data, record)

    def _finalize(self, experiment):
        config = getattr(self.config, 'finalize', None)
        return self.finalize(config, experiment)

    def __call__(self):
        log.info('Pipeline started. Loading data.')
        self._load()
        log.info('Data loaded. Starting models loop.')

        model_iterator = self._model_iterator()
        # see if self._model_iterator has len
        try:
            total = len(model_iterator)
        except:
            log.info('Number of models to train is unknown')
            total = None
        else:
            log.info('Models to train: {}'.format(total))

        if self._paralllel:
            self._concurrent_run(model_iterator, total)
        else:
            self._serial_run(model_iterator, total)

        log.info('Running finalize step.')
        self._finalize(self.ex)

    def _serial_run(self, model_iterator, total):
        for i, model in enumerate(model_iterator, 1):
            self._one_step(model, i, total)

    def _concurrent_run(self, model_iterator, total):
        from concurrent import futures
        with futures.ThreadPoolExecutor(self._workers) as executor:
            executor.map(self._one_step, model_iterator, range(total),
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
