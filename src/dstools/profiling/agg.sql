{% import 'macros.sql' as macros %}

with simple AS (
    {{macros.simple(relation=relation, mappings=mappings,
                    alias=alias, group_by=group_by)}}
), agg AS (
    SELECT {{macros.agg_columns(mappings=mappings, alias=alias, agg=agg)}}
    FROM simple
)

SELECT agg.* {% if return_all %} , {{macros.list_columns(mappings, alias, agg)}} {% endif %}
FROM agg {{ ', simple' if return_all else '' }}