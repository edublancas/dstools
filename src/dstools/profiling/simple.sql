{% from 'macros.sql' import simple %}

{{simple(relation=relation, mappings=mappings, alias=alias, group_by=group_by)}}
