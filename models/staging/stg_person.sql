with
    person as (
        select
            businessentityid
            , firstname
            , middlename
            , lastname
            , persontype
        from {{ source("sap_adw", "person") }}
    )
select *
from person