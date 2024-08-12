with
    address as (
        select
            addressid
            , stateprovinceid
            , city
        from {{ source("sap_adw", "address") }}
    )
select *
from address