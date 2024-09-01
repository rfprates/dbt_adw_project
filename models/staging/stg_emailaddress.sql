with
    emailaddress as (
        select
            businessentityid
            , cast(emailaddress.emailaddress as string) as email_address
        from {{ source("sap_adw", "emailaddress") }}
    )
select *
from emailaddress