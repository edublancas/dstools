"""
%load_ext autoreload
%autoreload 2
"""
from dstools.sql import infer


def test_detects_create_table_w_schema():
    ct = infer.created_tables("CREATE TABLE my_schema.my_table")
    assert len(ct) == 1 and ct[0] == ('my_schema', 'my_table', 'table')


def test_detects_create_table_wo_schema():
    ct = infer.created_tables("CREATE TABLE my_table")
    assert len(ct) == 1 and ct[0] == (None, 'my_table', 'table')
