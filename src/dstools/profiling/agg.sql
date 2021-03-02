{% import 'macros.sql' as macros %}

with simple AS (
    {{macros.simple(relation=relation, mappings=mappings,
                    alias=alias, group_by=group_by)}}
)

SELECT
{{macros.agg_columns(mappings=mappings, alias=alias, agg=agg)}}
FROM simple

