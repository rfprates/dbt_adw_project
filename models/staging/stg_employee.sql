with
    employee as (
        select
            businessentityid
            , loginid
        from {{ source("sap_adw", "employee") }}
    )
select *
from employee