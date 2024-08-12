with
    state_province as (
        select
            stateprovinceid
            , name as state_name
        from {{ source("sap_adw", "stateprovince") }}
    )
select *
from state_province