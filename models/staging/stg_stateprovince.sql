with
    state_province as (
        select
            stateprovinceid
            , countryregioncode
            , name as state
        from {{ source("sap_adw", "stateprovince") }}
    )
select *
from state_province