
  create view `public`.`traffic_avg_speed_by_type_model__dbt_tmp` as (
    -- Use the `ref` function to select from other models

select " type" , AVG(" avg_speed") 
from `public`.`traffic_dbt_model`
Group by " type"
  );