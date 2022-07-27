
-- Use the `ref` function to select from other models

select " type" , AVG(" traveled_d") 
from {{ ref('traffic_dbt_model') }}
Group by " type"
