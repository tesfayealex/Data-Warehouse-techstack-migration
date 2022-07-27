-- Use the `ref` function to select from other models

select " type" , AVG(" avg_speed") 
from "trial"."trial"."traffic_dbt_model"
Group by " type"