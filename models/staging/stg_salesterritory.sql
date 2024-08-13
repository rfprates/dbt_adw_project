with
    sales_territory as (
        select
            territoryid
            , countryregioncode
            , "group" as continent
        from {{ source("sap_adw", "salesterritory") }}
    )
select *
from sales_territory