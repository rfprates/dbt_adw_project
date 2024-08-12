with
    stg_address as (
        select
            addressid
            , stateprovinceid
            , city
        from {{ ref("stg_address") }}
    )
    , stg_state_province as (
        select
            stateprovinceid
            , countryregioncode
            , state
        from {{ ref("stg_stateprovince") }}
    )
    , stg_country_region as (
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
            , stg_state_province.state
            , stg_country_region.country
        from stg_address
        left join stg_state_province on stg_address.stateprovinceid = stg_state_province.stateprovinceid
        left join stg_country_region on stg_state_province.countryregioncode = stg_country_region.countryregioncode
    )
select *
from joined