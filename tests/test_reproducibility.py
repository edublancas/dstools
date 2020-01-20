from pathlib import Path
from dstools import reproducibility as repro


# def test_can_make_filename():
#     repro.make_filename()


# def test_make_logger_file(path_to_env, path_to_source_code_file, cleanup_env):
#     path_to_home = Env(path_to_env).path.home
#     path_to_log = repro.make_logger_file(path_to_source_code_file)
#     path_to_log_dir = path_to_log.relative_to(path_to_home).parent

#     assert path_to_log_dir == Path('log', 'src', 'pkg', 'module', 'functions')


# def test_setup_logger(path_to_env, path_to_source_code_file, cleanup_env):
#     repro.setup_logger(path_to_source_code_file)
