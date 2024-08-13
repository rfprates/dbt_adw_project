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
    , stg_salesterritory as (
        select
            territoryid
            , countryregioncode
            , group_territory
        from {{ ref('stg_salesterritory') }}
    )
    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['addressid']) }} as locations_sk
            , stg_address.addressid
            , stg_address.city
            , stg_stateprovince.state
            , stg_countryregion.country
            , stg_salesterritory.territoryid
            , stg_salesterritory.group_territory
        from stg_address
        left join stg_stateprovince on stg_address.stateprovinceid = stg_stateprovince.stateprovinceid
        left join stg_countryregion on stg_stateprovince.countryregioncode = stg_countryregion.countryregioncode
        left join stg_salesterritory on stg_countryregion.countryregioncode = stg_salesterritory.countryregioncode
    )
select *
from joined