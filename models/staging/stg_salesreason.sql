with
    sales_reason as (
        select
            salesreasonid
            , name as reason_description
            , reasontype as reason_type
        from {{ source("sap_adw", "salesreason") }}
    )
select *
from sales_reason