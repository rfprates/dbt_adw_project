with
    product as (
        select
            productid
            , name as product_name
            , makeflag
            , finishedgoodsflag
        from {{ source("sap_adw", "product") }}
    )
select *
from product