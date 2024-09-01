with
    sales_person as (
        select
            businessentityid
            , territoryid
            , salesquota
            , bonus 
            , salesytd
            , saleslastyear
            , commissionpct
        from {{ source("sap_adw", "salesperson") }}
    )
select *
from sales_person