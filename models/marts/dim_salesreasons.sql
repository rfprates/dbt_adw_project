with
    stg_salesorderheadersalesreason as (
        select
            salesorderid
            , salesreasonid
        from {{ ref("stg_salesorderheadersalesreason") }}
    )
    , stg_salesreason as (
        select
            salesreasonid
            , reason_name
        from {{ ref("stg_salesreason") }}
    )
    , reason_by_orderid as (
        select
            stg_salesorderheadersalesreason.salesorderid
            , stg_salesreason.reason_name as reason_name
        from stg_salesorderheadersalesreason
        left join stg_salesreason on stg_salesorderheadersalesreason.salesreasonid = stg_salesreason.salesreasonid
    )
    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['salesorderid']) }} as salesreasons_sk
            , salesorderid
            -- function used to aggregate in one row any multiple reasons attributed to a single salesorderid
            , string_agg(reason_name, ', ') as reason_name_aggregated
        from reason_by_orderid
        group by salesorderid
    )
select *
from joined