with
    employee as (
        select
            businessentityid
            , jobtitle
        from {{ source("sap_adw", "employee") }}
    )
select *
from employee