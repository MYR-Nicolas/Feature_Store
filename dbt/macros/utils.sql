-- Safely divides two expressions and prevents division by zero.
{% macro safe_divide(numerator, denominator) %}
    {{ numerator }} / NULLIF({{ denominator }}, 0)
{% endmacro %}