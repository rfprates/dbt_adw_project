/* test created to ensure that revenue colum on fct_sales model is not able to have negative or zero values */
with
    sales as (
        select
            sum(revenue) as total_revenue
            , orderdate
        from {{ ref('fct_sales') }}
        group by orderdate
        order by orderdate
    )
select *
from sales
where total_revenue <= 0