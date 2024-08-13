with
    sales_order_header as (
        select
            salesorderid
            , customerid
            , salespersonid
            , shiptoaddressid
            , creditcardid
            , territoryid
            , orderdate
            , status
        from {{ source("sap_adw", "salesorderheader") }}
    )
select *
from sales_order_header