with
    customer as (
        select
            customerid
            , storeid
            , personid
        from {{ source("sap_adw", "customer") }}
    )
select *
from customer