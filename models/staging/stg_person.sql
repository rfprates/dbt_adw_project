with
    person as (
        select
            businessentityid
            , firstname
            , middlename
            , lastname
            , persontype
            , emailpromotion
        from {{ source("sap_adw", "person") }}
    )
select *
from person