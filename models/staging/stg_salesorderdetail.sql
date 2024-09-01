with
    sales_order_detail as (
        select
            salesorderdetailid
            , salesorderid
            , productid
            , unitprice
            , orderqty
            , unitpricediscount
        from {{ source("sap_adw", "salesorderdetail") }}
    )
select *
from sales_order_detail