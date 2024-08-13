with
    sales_territory as (
        select
            territoryid
            , countryregioncode
            , `group` as group_territory
        from {{ source("sap_adw", "salesterritory") }}
    )
select *
from sales_territory