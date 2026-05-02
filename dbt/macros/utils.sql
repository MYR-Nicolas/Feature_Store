-- Safely divides two expressions and prevents division by zero.
{% macro safe_divide(numerator, denominator) %}
    SAFE_DIVIDE({{ numerator }}, NULLIF({{ denominator }}, 0))
{% endmacro %}