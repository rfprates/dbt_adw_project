with
    person_credit_card as (
        select
            businessentityid
            , creditcardid
        from {{ source("sap_adw", "personcreditcard") }}
    )
select *
from person_credit_card