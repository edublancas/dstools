from dstools.lab import Experiment, SKExperiment
from dstools.util import hash_sha1_numpy_array, class_name
import logging
import collections

from dstools.sklearn.util import model_name

log = logging.getLogger(__name__)
MAX_WORKERS = 20


class Pipeline(object):
    _ExperimentClass = Experiment

    def __init__(self, config, exp_config, workers=1, save=True,
                 hash_data=True):
        log.debug('Init with config: {}'.format(config))

        if workers > MAX_WORKERS:
            self._workers = MAX_WORKERS
            log.info('Max workers is {}.'.format(MAX_WORKERS))
        else:
            self._workers = workers

        self._save = save
        self._hash_data = hash_data
        self.config = config

        # initialize dict to save the data hashes
        self._data_hashes = {}

        # initialize functions as None
        self.load = None
        self.model_iterator = None
        self.train = None
        self.finalize = None
        # create experiment instance
        self.ex = self.__class__._ExperimentClass(**exp_config)

    def _load(self):
        config = self.config.get('load')
        data = self.load(config)
        if isinstance(data, collections.Mapping):
            self.data = data
        else:
            raise TypeError(('Object returned from self.load method should be'
                             ' a Mapping class. e.g. dict'))

        # save the hash of the datasets if hash_data is True
        # although we are not saving it on the experiment instance
        # right now, is better to raise an exception early if something
        # goes wrong
        if self._hash_data:
            for k, v in self.data.items():
                log.info('Hashing {}'.format(k))
                try:
                    h = hash_sha1_numpy_array(v)
                except Exception, e:
                    raise e
                else:
                    key = '{}'.format(k)
                    self._data_hashes[key] = h

    def _model_iterator(self):
        config = self.config.get('model_iterator')
        log.debug('Model iterator config: {}'.format(config))

        return self.model_iterator(config)

    def _train(self, model, record):
        record['_model_class'] = class_name(model)
        config = self.config.get('train')
        self.train(config, model, self.data, record)

    def _finalize(self, experiment):
        # save config used for this experiment on all records
        self.ex['_config'] = self.config

        # save data hashes if needed
        if self._hash_data:
            self.ex['_data_sha1_hashes'] = self._data_hashes

        # run function if the user provided one
        if self.finalize:
            config = self.config.get('finalize')
            self.finalize(config, experiment)

    def __call__(self):
        # first - check if all the functions needed exist
        if not all([self.load, self.model_iterator, self.train]):
            raise Exception(('You need to provide functions for load,'
                             ' model_iterator and train functions.'
                             ' One or more missing.'))

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

        if self._workers > 1:
            self._concurrent_run(model_iterator, total)
        else:
            self._serial_run(model_iterator, total)

        log.info('Running finalize step.')
        self._finalize(self.ex)

        if self._save:
            self.ex.save()

    def _serial_run(self, model_iterator, total):
        for i, model in enumerate(model_iterator, 1):
            self._one_step(model, i, total)

    def _concurrent_run(self, model_iterator, total):
        from concurrent import futures
        # maybe multiprocessing would be better for this
        with futures.ThreadPoolExecutor(self._workers) as executor:
            executor.map(self._one_step, model_iterator, range(1, total),
                         [total]*total)

    def _one_step(self, model, i, total):
        if total:
            log.info('{}/{} - Running with: {}'.format(i, total, model))
        else:
            log.info('{} - Running with: {}'.format(i, model))

        record = self.ex.record()
        self._train(model, record)


class SKPipeline(Pipeline):
    '''
        Pipeline subclass. Provides enhanced functionality
        when using scikit-learn.
            - Automatically saves model name e.g. RandomForestClassifier
            - Saves model parameters via model.get_params()
            - Uses Record subclass SKRecord which provides a method
                for instantiating models based on db records
    '''
    _ExperimentClass = SKExperiment

    def _train(self, model, record):
        super(SKPipeline, self)._train(model, record)
        # save scikit-learn model info
        record['_params'] = model.get_params()
        record['_model_name'] = model_name(model)
        # try to get feature importances
        try:
            record['_feature_importances'] = (model.feature_importances_.
                                              tolist())
        except:
            pass
        # try to get coefficients
        try:
            record['_coef'] = model.coef_.tolist()
        except:
            pass
