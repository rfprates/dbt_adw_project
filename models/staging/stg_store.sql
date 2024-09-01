with
    store as (
        select
            businessentityid
            , name as store
        from {{ source("sap_adw", "store") }}
    )
select *
from store