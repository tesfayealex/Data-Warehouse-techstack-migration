
  create view "trial"."trial"."traffic_avg_distance_by_type_model__dbt_tmp" as (
    -- Use the `ref` function to select from other models

select " type" , AVG(" traveled_d") 
from "trial"."trial"."traffic_dbt_model"
Group by " type"
  );