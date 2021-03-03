{% import 'macros.sql' as macros %}

with simple AS (
    {{macros.simple(relation=relation, mappings=mappings,
                    alias=alias, group_by=group_by)}}
), agg AS (
    SELECT {{macros.agg_columns(mappings=mappings, alias=alias, agg=agg, group_by=group_by)}}
    FROM simple
) {% if return_all %}, simple_no_agg AS (
    {{macros.simple(relation=relation, mappings=mappings,
                    alias=alias, group_by=None)}}
) {% endif %}

SELECT *
FROM agg {{ ', simple_no_agg' if return_all else '' }}