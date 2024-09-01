with
    product as (
        select
            productid
            , name as product_name
            , finishedgoodsflag
            , productsubcategoryid
        from {{ source("sap_adw", "product") }}
    )
select *
from product