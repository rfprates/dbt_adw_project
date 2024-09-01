with
    stg_customer as (
        select
            customerid
            , storeid
        from {{ ref('stg_customer') }}
    )
    , stg_store as (
        select
            businessentityid as storeid
            , store
        from {{ ref('stg_store') }}
    )
    , join_customer_store as (
        select
            customerid
            , case
                /* Also selecting online orders, describing the store name as "Online Sell" */
                when stg_store.store is null
                then 'Online Sell'
                else stg_store.store
            end as store_name
        from stg_customer
        left join stg_store on stg_customer.storeid = stg_store.storeid
    )
    , dim_products as (
        select
            products_sk
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
            customerid
            , products_fk
            , locations_fk
            , orderqty
            , orderdate
        from {{ ref('fct_sales') }}
    )
    , agg_model as (
        select
            fct_sales.orderdate
            , join_customer_store.store_name
            , dim_products.product
            , dim_locations.province
            , dim_locations.country
            , fct_sales.orderqty
        from fct_sales
        left join join_customer_store on fct_sales.customerid = join_customer_store.customerid
        left join dim_products on fct_sales.products_fk = dim_products.products_sk
        left join dim_locations on fct_sales.locations_fk = dim_locations.locations_sk
        order by orderdate asc
    )
select *
from agg_model