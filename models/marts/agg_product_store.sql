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
            , product_name
        from {{ ref('dim_products') }}
    )
    , stg_countryregion as (
        select
            countryregioncode
            , country
        from {{ ref('stg_countryregion') }}
    )
    , stg_salesterritory as (
        select
            territoryid
            , countryregion as region
            , countryregioncode
        from {{ ref('stg_salesterritory') }}
    )
    , fct_sales as (
        select
            customers_fk
            , products_fk
            , territoryid
            , orderqty
            , orderdate
        from {{ ref('fct_sales') }}
    )
    , country_region as (
        select
            stg_salesterritory.territoryid
            , stg_salesterritory.region
            , stg_countryregion.country
        from stg_salesterritory
        left join stg_countryregion on stg_salesterritory.countryregioncode = stg_countryregion.countryregioncode
    )
    , agg_model as (
        select
            fct_sales.orderdate
            , dim_customers.storeid
            , dim_customers.store
            , dim_products.productid
            , dim_products.product_name
            , country_region.region
            , country_region.country
            , fct_sales.orderqty
        from fct_sales
        left join dim_customers on fct_sales.customers_fk = dim_customers.customers_sk
        left join dim_products on fct_sales.products_fk = dim_products.products_sk
        left join country_region on fct_sales.territoryid = country_region.territoryid
        where dim_customers.storeid is not null
        order by orderdate asc
    )
select *
from agg_model