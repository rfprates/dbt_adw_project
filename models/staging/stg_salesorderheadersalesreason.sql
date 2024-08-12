with
    sales_order_header_sales_reason as (
        select
            salesorderid
            , salesreasonid
        from {{ source("sap_adw", "salesorderheadersalesreason") }}
    )
select *
from sales_order_header_sales_reason