with
    sales_territory as (
        select
            territoryid
            , name as countryregion
            , countryregioncode
            , `group` as territorygroup
            , salesytd
            , saleslastyear
        from {{ source("sap_adw", "salesterritory") }}
    )
select *
from sales_territory