with
    emailaddress as (
        select
            businessentityid
            , emailaddress
        from {{ source("sap_adw", "emailaddress") }}
    )
select *
from emailaddress