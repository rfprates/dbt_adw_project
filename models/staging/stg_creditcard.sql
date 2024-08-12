with
    credit_card as (
        select
            creditcardid
            , cardtype
        from {{ source("sap_adw", "creditcard") }}
    )
select *
from credit_card