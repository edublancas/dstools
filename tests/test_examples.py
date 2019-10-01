"""
Runs examples form examples/
"""
import subprocess


def test_pipeline_runs(tmp_intermediate_example_directory):
    assert subprocess.call(['python', 'pipeline.py']) == 0
