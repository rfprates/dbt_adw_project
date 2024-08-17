with customers as (
    select
        customers_sk
        , customerid
    from {{ ref('dim_customers') }} 
)
, salespersons as (
    select
        salespersons_sk
        , salespersonid
    from {{ ref('dim_salespersons') }}
)
, locations as (
    select
        locations_sk
        , addressid
    from {{ ref('dim_locations') }}
)
, salesreasons as (
    select
        salesreasons_sk
        , salesorderid
    from {{ ref('dim_salesreasons') }}
)
, products as (
    select
        products_sk
        , productid
    from {{ ref('dim_products') }}
)

/* Import stg_salesordetail */
, stg_salesorderdetail as (
    select
        *
    from {{ ref ('stg_salesorderdetail') }}
)
/* Using stg_salesordetail to join with dim_products, saving products_sk as products_fk */
, salesorderdetail as (
    select
        stg_salesorderdetail.salesorderdetailid
        , stg_salesorderdetail.salesorderid
        , products.products_sk as products_fk
        , stg_salesorderdetail.orderqty
        , stg_salesorderdetail.unitprice
        , stg_salesorderdetail.unitprice * stg_salesorderdetail.orderqty as revenue
    from stg_salesorderdetail
    left join products on stg_salesorderdetail.productid = products.productid
)

/* Import stg_salesorderheader */
, stg_salesorderheader as (
    select
        *
    from {{ ref('stg_salesorderheader') }}
)
/* Using stg_salesorderheader to joint with dim_customers, dim_salespersons, dim_locations and dim_salesreasons, saving surrogate keys of each dim tables */
, salesorderheader as (
    select
        stg_salesorderheader.salesorderid
        , customers.customers_sk as customers_fk
        , salespersons.salespersons_sk as salespersons_fk
        , locations.locations_sk as locations_fk
        , salesreasons.salesreasons_sk as salesreasons_fk
        /* Description added to order_status based on column descriptions */
        , case
            when stg_salesorderheader.status = 1 THEN 'In_process'
            WHEN stg_salesorderheader.status = 2 THEN 'Approved'
            WHEN stg_salesorderheader.status = 3 THEN 'Backordered' 
            WHEN stg_salesorderheader.status = 4 THEN 'Rejected' 
            WHEN stg_salesorderheader.status = 5 THEN 'Shipped'
            WHEN stg_salesorderheader.status = 6 THEN 'Cancelled' 
            ELSE 'no_status'
        end as status_description
        , stg_salesorderheader.orderdate
    from stg_salesorderheader 
    left join customers on stg_salesorderheader.customerid = customers.customerid
    left join salespersons on stg_salesorderheader.salespersonid = salespersons.salespersonid
    left join locations on stg_salesorderheader.shiptoaddressid = locations.addressid
    left join salesreasons on stg_salesorderheader.salesorderid = salesreasons.salesorderid
)

/* Join of salesorderdetail and salesorderheader to get the final fact table */
, final as (
    select
        salesorderdetail.products_fk
        , salesorderheader.customers_fk
        , salesorderheader.locations_fk
        , salesorderheader.salespersons_fk
        , salesorderheader.salesreasons_fk
        , salesorderdetail.salesorderid
        , salesorderdetail.unitprice
        , salesorderdetail.orderqty
        , salesorderdetail.revenue
        , salesorderheader.status_description
        , salesorderheader.orderdate
    from salesorderdetail
    left join salesorderheader on salesorderdetail.salesorderid = salesorderheader.salesorderid
)

select *
from final