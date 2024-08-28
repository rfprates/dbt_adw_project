with
    dim_customers as (
        select
            customers_sk
            , storeid
            , store
        from {{ ref('dim_customers') }}
    )
    , dim_products as (
        select
            products_sk
            , productid
            , product_name as product
        from {{ ref('dim_products') }}
    )
    , dim_locations as (
        select
            locations_sk
            , state as province
            , country
        from {{ ref('dim_locations') }}
    )
    , fct_sales as (
        select
            customers_fk
            , products_fk
            , locations_fk
            , orderqty
            , orderdate
        from {{ ref('fct_sales') }}
    )
    , agg_model as (
        select
            fct_sales.orderdate
            , dim_customers.storeid
            , dim_customers.store
            , dim_products.productid
            , dim_products.product
            , dim_locations.province
            , dim_locations.country
            , fct_sales.orderqty
        from fct_sales
        left join dim_customers on fct_sales.customers_fk = dim_customers.customers_sk
        left join dim_products on fct_sales.products_fk = dim_products.products_sk
        left join dim_locations on fct_sales.locations_fk = dim_locations.locations_sk
        where dim_customers.storeid is not null
        order by orderdate asc
    )
select *
from agg_model