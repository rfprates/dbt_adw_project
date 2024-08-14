with
    sales_territory as (
        select
            territoryid
            , countryregioncode
            , name as countryregion
            , `group` as territorygroup
        from {{ source("sap_adw", "salesterritory") }}
    )
select *
from sales_territory