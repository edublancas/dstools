"""
Analyzes SQL scripts to infer performed actions
"""
import logging
import re


def find_with_positions(regex, s):
    return {m.groups(): m.start() for m in regex.finditer(s)}


def find_created_table(template_render):
    logger = logging.getLogger(__name__)

    re_created = re.compile(r".*CREATE TABLE (\w+)\.{1}(\w+).*")
    created = find_with_positions(re_created, template_render)

    re_dropped = re.compile(r".*DROP TABLE (?:IF EXISTS )?(\w+)\.{1}(\w+).*")
    dropped = find_with_positions(re_dropped, template_render)

    stay = []

    # the script might be creating temporary tables, so remove the created
    # tables that also have drop satements but only when the DROP comes after
    # the CREATE
    for g, pos_created in created.items():
        pos_dropped = dropped.get(g, -1)

        if pos_created > pos_dropped:
            stay.append(g)

    logger.info(f'CREATE TABLE statements found: {created}')
    logger.info(f'DROP TABLE statements found: {dropped}')

    logger.info(f'Script will create the tables: {stay}')

    # NOTE: we are just returning the first element, since all tables created
    # with the same script will have the same metadata we really just have to
    # check one, however, a cleaner version should check all
    return None if not len(stay) else stay[0]


def find_created_view(template_render):
    logger = logging.getLogger(__name__)

    re_created = re.compile(r".*CREATE VIEW (\w+)\.{1}(\w+).*")
    created = find_with_positions(re_created, template_render)

    re_dropped = re.compile(r".*DROP VIEW (?:IF EXISTS )?(\w+)\.{1}(\w+).*")
    dropped = find_with_positions(re_dropped, template_render)

    stay = []

    for g, pos_created in created.items():
        pos_dropped = dropped.get(g, -1)

        if pos_created > pos_dropped:
            stay.append(g)

    logger.info(f'CREATE VIEW statements found: {created}')
    logger.info(f'DROP VIEW statements found: {dropped}')

    logger.info(f'Script will create the views: {stay}')

    # NOTE: we are just returning the first element, since all tables created
    # with the same script will have the same metadata we really just have to
    # check one, however, a cleaner version should check all
    return None if not len(stay) else stay[0]


def find_created_relation(template_render):
    res = find_created_table(template_render)

    if res is not None:
        schema, name = res
        return schema, name, 'table'

    res = find_created_view(template_render)

    if res is not None:
        schema, name = res
        return schema, name, 'view'

    raise ValueError("Couldn't find any "
                     "CREATE [TABLE|VIEW] AS in the template: "
                     f"{template_render}")