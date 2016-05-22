from dstools import ExperimentLogger

# create logger instance
exp_logger = ExperimentLogger()

@exp_logger.record_with_suffix
def do_stuff(a, b_save):
    c_save = 'x'
    pass

do_stuff(10, b_save=10)