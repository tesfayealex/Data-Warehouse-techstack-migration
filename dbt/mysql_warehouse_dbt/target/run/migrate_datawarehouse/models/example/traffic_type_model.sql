
  create view `public`.`traffic_type_model__dbt_tmp` as (
    -- Use the `ref` function to select from other models

select " type" , COUNT(id) 
from `public`.`traffic_dbt_model`
Group by " type"
  );