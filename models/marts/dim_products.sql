with
    stg_product as (
        select
            productid
            , product_name
            , makeflag
            , finishedgoodsflag
            , productsubcategoryid
        from {{ ref('stg_product') }}
    )
    , stg_productsubcategory as (
        select
            productsubcategoryid
            , productcategoryid
            , name as subcategory_name
        from {{ ref('stg_productsubcategory') }}
    )
    , stg_productcategory as (
        select
            productcategoryid
            , name as category_name
        from {{ ref('stg_productcategory') }}
    )
    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['productid', 'product_name']) }} as products_sk
            , stg_product.productid
            , stg_product.product_name
            , stg_product.makeflag
            , stg_product.finishedgoodsflag
            , stg_productsubcategory.subcategory_name
            , stg_productcategory.category_name
        from stg_product
        left join stg_productsubcategory on stg_product.productsubcategoryid = stg_productsubcategory.productsubcategoryid
        left join stg_productcategory on stg_productsubcategory.productcategoryid = stg_productcategory.productcategoryid
    )
select *
from joined