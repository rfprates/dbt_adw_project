with
    stg_address as (
        select
            addressid
            , stateprovinceid
            , city
        from {{ ref("stg_address") }}
    )
    , stg_stateprovince as (
        select
            stateprovinceid
            , countryregioncode
            , state
        from {{ ref("stg_stateprovince") }}
    )
    , stg_countryregion as (
        select
            countryregioncode
            , country
        from {{ ref("stg_countryregion") }}
    )
    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['addressid']) }} as locations_sk
            , stg_address.addressid
            , stg_address.city
            , stg_stateprovince.state
            , stg_countryregion.country
        from stg_address
        left join stg_stateprovince on stg_address.stateprovinceid = stg_stateprovince.stateprovinceid
        left join stg_countryregion on stg_stateprovince.countryregioncode = stg_countryregion.countryregioncode
    )
select *
from joined