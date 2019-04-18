"""
Analyzes SQL scripts to infer performed actions
"""
import logging
import re


def find_with_positions(regex, s):
    def clean(schema, name):
        if schema is not None:
            schema = schema.replace('.', '')
        return schema, name

    return {clean(*m.groups()): m.start() for m in regex.finditer(s)}


def created_tables(sql):
    logger = logging.getLogger(__name__)

    re_created = re.compile(r".*CREATE TABLE (\w+\.{1})?(\w+).*")
    created = find_with_positions(re_created, sql)

    re_dropped = re.compile(r".*DROP TABLE (?:IF EXISTS )?(\w+\.{1})?(\w+).*")
    dropped = find_with_positions(re_dropped, sql)

    stay = []

    # the script might be creating temporary tables, so remove the created
    # tables that also have drop satements but only when the DROP comes after
    # the CREATE
    for g, pos_created in created.items():
        pos_dropped = dropped.get(g, -1)

        if pos_created > pos_dropped:
            stay.append((*g, 'table'))

    logger.info(f'CREATE TABLE statements found: {created}')
    logger.info(f'DROP TABLE statements found: {dropped}')

    logger.info(f'Script will create the tables: {stay}')

    return stay


def created_views(sql):
    logger = logging.getLogger(__name__)

    re_created = re.compile(r".*CREATE VIEW (\w+\.{1})?(\w+).*")
    created = find_with_positions(re_created, sql)

    re_dropped = re.compile(r".*DROP VIEW (?:IF EXISTS )?(\w+\.{1})?(\w+).*")
    dropped = find_with_positions(re_dropped, sql)

    stay = []

    for g, pos_created in created.items():
        pos_dropped = dropped.get(g, -1)

        if pos_created > pos_dropped:
            stay.append((*g, 'view'))

    logger.info(f'CREATE VIEW statements found: {created}')
    logger.info(f'DROP VIEW statements found: {dropped}')

    logger.info(f'Script will create the views: {stay}')

    return stay


def created_relations(sql):
    return created_tables(sql) + created_views(sql)
