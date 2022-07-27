
  create view "trial"."trial"."traffic_avg_speed_by_type_model__dbt_tmp" as (
    -- Use the `ref` function to select from other models

select " type" , AVG(" avg_speed") 
from "trial"."trial"."traffic_dbt_model"
Group by " type"
  );