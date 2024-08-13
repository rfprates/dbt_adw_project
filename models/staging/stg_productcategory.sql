with
    product_category as (
        select
            productcategoryid
            , name
        from {{ source("sap_adw", "productcategory") }}
    )
select *
from product_category