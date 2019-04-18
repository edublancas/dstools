"""
%load_ext autoreload
%autoreload 2
"""
from dstools.sql import infer


def test_detects_create_table_w_schema():
    assert (infer.created_table("CREATE TABLE my_schema.my_table") ==
            ('my_schema', 'my_table'))


def test_detects_create_table_wo_schema():
    assert (infer.created_table("CREATE TABLE my_table") ==
            (None, 'my_table'))
