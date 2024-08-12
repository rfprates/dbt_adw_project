with
    sales_person as (
        select
            businessentityid
            , salesytd
            , saleslastyear
        from {{ source("sap_adw", "salesperson") }}
    )
select *
from sales_person