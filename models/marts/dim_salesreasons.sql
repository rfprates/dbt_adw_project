with
    stg_sales_order_header_sales_reason as (
        select
            salesorderid
            , salesreasonid as salesreason_fk
        from {{ ref("stg_salesorderheadersalesreason") }}
    )
    , stg_sales_reason as (
        select
            salesreasonid as salesreason_pk
            , reason_description
            , reason_type
        from {{ ref("stg_salesreason") }}
    )
    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['salesorderid', 'salesreason_fk']) }} as salesreason_sk
            , salesorderid
            , reason_description
            , reason_type
        from stg_sales_order_header_sales_reason
        left join stg_sales_reason on stg_sales_order_header_sales_reason.salesreason_fk = stg_sales_reason.salesreason_pk
    )
select *
from joined