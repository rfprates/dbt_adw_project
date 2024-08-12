with
    sales_order_header as (
        select
            salesorderid
            , customerid
            , salespersonid
            , billtoaddressid
            , creditcardid
            , orderdate
            , status
        from {{ source("sap_adw", "salesorderheader") }}
    )
select *
from sales_order_header