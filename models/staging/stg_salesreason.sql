with
    sales_reason as (
        select
            salesreasonid
            , name as reason_name
        from {{ source("sap_adw", "salesreason") }}
    )
select *
from sales_reason