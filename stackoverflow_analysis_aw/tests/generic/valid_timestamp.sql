{% test valid_timestamp(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} > CURRENT_TIMESTAMP
  or {{ column_name }} < '2008-01-01'

{% endtest %}