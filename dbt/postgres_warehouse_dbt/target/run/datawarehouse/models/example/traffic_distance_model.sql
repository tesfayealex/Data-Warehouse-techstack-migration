
  create view "trial"."trial"."traffic_distance_model__dbt_tmp" as (
    -- Use the `ref` function to select from other models

select *
from "trial"."trial"."traffic_dbt_model"
where id = 1
  );