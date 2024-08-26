with
    stg_salesperson as (
        select 
            businessentityid as salespersonid
            , territoryid
            , salesquota
            , bonus 
            , salesytd
            , saleslastyear
            , commissionpct
        from {{ ref('stg_salesperson') }}
    )
    , stg_employee as (
        select
            businessentityid as employeebusinessentityid
            , jobtitle
        from {{ ref('stg_employee') }}
    )
    , stg_person as (
        select
            businessentityid as personbusinessentityid
            , concat(ifnull(firstname,' '),' ',ifnull(middlename,' '),' ',ifnull(lastname,' ')) as fullname
            , case
                when persontype = "SC" then "Store contact"
                when persontype = "IN" then "Individual customer"
                when persontype = "SP" then "Sales person"
                when persontype = "EM" then "Employee"
                when persontype = "VC" then "Vendor contact"
                when persontype = "GC" then "General contact"
            end as persondescription
        from {{ ref('stg_person') }}
    )
    , fct_sales as (
        select
            salespersonid
            , salesorderid
            , orderqty
            , revenue_with_discount
            , orderdate
        from {{ ref('fct_sales') }}
    )
    , agg_sales as (
        select
            salespersonid
            , sum(revenue_with_discount) as sales2014
        from fct_sales
        where extract(year from orderdate) = 2014
        group by salespersonid
    )
    , agg_orderid as (
        select
            salespersonid
            , count(distinct(salesorderid)) as qty_salesorderid
        from fct_sales
        group by salespersonid
    )
    , agg_orderqty as (
        select
            salespersonid
            , sum(orderqty) as qty_sold
        from fct_sales
        group by salespersonid
    )
    /* selecting the most sold product by each of salesperson */
    , dim_products as (
        select
            productid
            , product_name
            , subcategory_name
            , category_name
        from {{ ref('dim_products') }}
    )
    , stg_salesorderheader as (
        select
            salesorderid
            , salespersonid
        from {{ ref('stg_salesorderheader') }}
    )
    , stg_salesorderdetail as (
        select
            salesorderid
            , productid
            , orderqty
        from {{ ref('stg_salesorderdetail') }}
    )
    , count_product as (
        select
            salespersonid
            , productid
            , sum(orderqty) as qtysold
        from stg_salesorderdetail
        left join stg_salesorderheader on stg_salesorderdetail.salesorderid = stg_salesorderheader.salesorderid
        group by salespersonid, productid
    )
    , ranking_count_product as (
        select
            salespersonid
            , productid
            , qtysold
            , rank() over (partition by salespersonid order by qtysold desc) as ranking
        from count_product
    )
    , max_product as (
        select
            salespersonid
            , product_name as best_selling_product
            , subcategory_name as subcategory
            , category_name as category
        from ranking_count_product
        left join dim_products on ranking_count_product.productid = dim_products.productid
        where ranking = 1
    )
    /* final query to join each CTE above */
    , final_query as (
        select
            stg_salesperson.salespersonid
            , stg_person.fullname
            , stg_employee.jobtitle
            , stg_person.persondescription
            , stg_salesperson.salesquota
            , stg_salesperson.bonus
            , stg_salesperson.commissionpct
            , stg_salesperson.salesytd
            , stg_salesperson.saleslastyear
            , agg_sales.sales2014
            , agg_orderid.qty_salesorderid
            , agg_orderqty.qty_sold
            , max_product.best_selling_product
            , max_product.subcategory
            , max_product.category
        from stg_salesperson
        left join stg_employee on stg_salesperson.salespersonid = stg_employee.employeebusinessentityid
        left join stg_person on stg_employee.employeebusinessentityid = stg_person.personbusinessentityid
        left join agg_sales on stg_salesperson.salespersonid = agg_sales.salespersonid
        left join agg_orderid on stg_salesperson.salespersonid = agg_orderid.salespersonid
        left join agg_orderqty on stg_salesperson.salespersonid = agg_orderqty.salespersonid
        left join max_product on stg_salesperson.salespersonid = max_product.salespersonid
    )
select *
from final_query