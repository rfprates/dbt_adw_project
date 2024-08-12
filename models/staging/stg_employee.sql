with
    employee as (
        select
            businessentityid
        from {{ source("sap_adw", "employee") }}
    )
select *
from employee