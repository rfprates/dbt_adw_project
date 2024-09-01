with
    product_subcategory as (
        select
            productsubcategoryid
            , productcategoryid
            , name
        from {{ source("sap_adw", "productsubcategory") }}
    )
select *
from product_subcategory