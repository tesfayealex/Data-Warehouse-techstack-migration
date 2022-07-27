
  create view "trial"."trial"."traffic_type_model__dbt_tmp" as (
    -- Use the `ref` function to select from other models

select " type" , COUNT(id) 
from "trial"."trial"."traffic_dbt_model"
Group by " type"
  );