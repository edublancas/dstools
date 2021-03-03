{% macro simple(relation, mappings, alias, group_by) -%}

{% set suffix = '' if not group_by else '_by_'+group_by %}

SELECT
    {% if group_by %} {{group_by}}, {% endif %}

    {% for col, functions in mappings.items() %}
        {% for fn in functions %}
            {% set alias = col if col not in alias else alias[col] %}
            {% if fn == 'count-distinct' %}
                COUNT(DISTINCT({{col}})) distinct_{{alias}}{{suffix}},
            {% else %}
                {{fn}}({{col}}) {{fn}}_{{alias}}{{suffix}},
            {% endif %}
        {% endfor %}
    {% endfor %}
                COUNT(*) count{{suffix}}

FROM {{relation}}
{% if group_by %} GROUP BY {{group_by}} {% endif %}
{%- endmacro %}



{% macro agg_columns(mappings, alias, agg, group_by) -%}

{% set suffix = '' if not group_by else '_by_'+group_by %}

{% for OUTER_AGG in agg %}
    {% for col, functions in mappings.items() %}
        {% for fn in functions %}
            {% set alias = col if col not in alias else alias[col] %}
            {% if fn == 'count-distinct' %}
                {{OUTER_AGG}}(distinct_{{alias}}{{suffix}}) {{OUTER_AGG}}_distinct_{{alias}}{{suffix}},
            {% else %}
                {{OUTER_AGG}}({{fn}}_{{alias}}{{suffix}}) {{OUTER_AGG}}_{{fn}}_{{alias}}{{suffix}},
            {% endif %}
        {% endfor %}
    {% endfor %}
            {{OUTER_AGG}}(count{{suffix}}) {{OUTER_AGG}}_count{{suffix}} {{'' if loop.last else ','}}
{% endfor %}

{%- endmacro %}



{% macro list_columns(mappings, alias, agg, group_by) -%}

{% set suffix = '' if not group_by else '_by_'+group_by %}

{% for col, functions in mappings.items() %}
    {% for fn in functions %}
        {% set alias = col if col not in alias else alias[col] %}
        {% if fn == 'count-distinct' %}
            distinct_{{alias}}{{suffix}},
        {% else %}
            {{fn}}_{{alias}}{{suffix}},
        {% endif %}
    {% endfor %}
{% endfor %}
            count{{suffix}}

{%- endmacro %}

