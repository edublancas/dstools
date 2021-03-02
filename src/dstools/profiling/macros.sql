{% macro simple(relation, mappings, alias, group_by=none) -%}

SELECT
    {% if group_by %} {{group_by}}, {% endif %}

    {% for col, functions in mappings.items() %}
        {% for fn in functions %}
            {% set alias = col if col not in alias else alias[col] %}
            {% if fn == 'count-distinct' %}
                COUNT(DISTINCT({{col}})) AS distinct_{{alias}},
            {% else %}
                {{fn}}({{col}}) AS {{fn}}_{{alias}},
            {% endif %}
        {% endfor %}
    {% endfor %}
                COUNT(*) AS n_rows

FROM {{relation}}
{% if group_by %} GROUP BY {{group_by}} {% endif %}
{%- endmacro %}



{% macro agg_columns(mappings, alias, agg) -%}

{% for OUTER_AGG in agg %}
    {% for col, functions in mappings.items() %}
        {% for fn in functions %}
            {% set alias = col if col not in alias else alias[col] %}
            {% if fn == 'count-distinct' %}
                {{OUTER_AGG}}(distinct_{{alias}}) AS {{OUTER_AGG}}_distinct_{{alias}},
            {% else %}
                {{OUTER_AGG}}({{fn}}_{{alias}}) AS {{OUTER_AGG}}_{{fn}}_{{alias}},
            {% endif %}
        {% endfor %}
    {% endfor %}
            {{OUTER_AGG}}(n_rows) AS {{OUTER_AGG}}_n_rows {{'' if loop.last else ','}}
{% endfor %}
{%- endmacro %}

