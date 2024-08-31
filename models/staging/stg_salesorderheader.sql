with
    sales_order_header as (
        select
            salesorderid
            , customerid
            , salespersonid
            , shiptoaddressid
            , territoryid
            , orderdate
            , status
            , onlineorderflag
        from {{ source("sap_adw", "salesorderheader") }}
    )
select *
from sales_order_header