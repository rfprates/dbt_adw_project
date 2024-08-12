with
    country_region as (
        select
            countryregioncode
            , name as country_name
        from {{ source("sap_adw", "countryregion") }}
    )
select *
from country_region