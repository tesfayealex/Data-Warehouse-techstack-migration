
  create view `public`.`traffic_dbt_model__dbt_tmp` as (
    

with traffic_dbt_model as (

    select * from public.import_data

)

select *
from traffic_dbt_model

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
  );