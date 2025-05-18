{% test valid_id_range(model, column_name) %}

with unusual_ids as (
    select 
        {{ column_name }} as id_value,
        case
            when {{ column_name }} < 0 then 'negative_id'
            when {{ column_name }} > 100000000 then 'suspiciously_high_id'
            else 'valid_id'
        end as issue_type
    from {{ model }}
    where {{ column_name }} < 0  -- catch negative IDs
       or {{ column_name }} > 100000000  -- catch suspiciously high IDs
)

select *
from unusual_ids
where issue_type != 'valid_id'

{% endtest %}