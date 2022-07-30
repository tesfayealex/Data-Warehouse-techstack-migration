
{{ config(materialized='view') }}

with traffic_dbt_model as (

    select * from datasets

)

select *
from traffic_dbt_model

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null