with
    stg_product as (
        select
            productid
            , product_name
            , case
                when finishedgoodsflag = false then 'Product is not a salable item'
                when finishedgoodsflag = true then 'Product is salable'
            end as product_salable
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
            , stg_productsubcategory.subcategory_name
            , stg_productcategory.category_name
        from stg_product
        left join stg_productsubcategory on stg_product.productsubcategoryid = stg_productsubcategory.productsubcategoryid
        left join stg_productcategory on stg_productsubcategory.productcategoryid = stg_productcategory.productcategoryid
        /* selecting only the salable products */
        where stg_product.product_salable = 'Product is salable'
    )
select *
from joined