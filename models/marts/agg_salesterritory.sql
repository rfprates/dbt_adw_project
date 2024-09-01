with
    stg_salesterritory as (
        select
            territoryid
            , countryregion
            , territorygroup
            , salesytd
            , saleslastyear
        from {{ ref('stg_salesterritory')}}
    )
    , fct_sales as (
        select
            territoryid
            , salesorderid
            , orderqty
            , revenue_with_discount
            , orderdate
        from {{ ref('fct_sales') }}
    )
    , agg_sales as (
        select
            territoryid
            , sum(revenue_with_discount) as sales2014
        from fct_sales
        where extract(year from orderdate) = 2014
        group by territoryid
    )
    , agg_orderid as (
        select
            territoryid
            , count(distinct(salesorderid)) as qty_salesorderid
        from fct_sales
        group by territoryid
    )
    , agg_orderqty as (
        select
            territoryid
            , sum(orderqty) as qty_sold
        from fct_sales
        group by territoryid
    )
    , joined as (
        select
            stg_salesterritory.territoryid
            , stg_salesterritory.countryregion
            , stg_salesterritory.territorygroup
            , stg_salesterritory.salesytd
            , stg_salesterritory.saleslastyear
            , agg_sales.sales2014
            , agg_orderid.qty_salesorderid
            , agg_orderqty.qty_sold
        from stg_salesterritory
        left join agg_sales on stg_salesterritory.territoryid = agg_sales.territoryid
        left join agg_orderid on stg_salesterritory.territoryid = agg_orderid.territoryid
        left join agg_orderqty on stg_salesterritory.territoryid = agg_orderqty.territoryid
    )
select *
from joined