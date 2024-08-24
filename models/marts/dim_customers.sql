with
    stg_customer as (
        select 
            customerid
            , personid
            , storeid
        from {{ ref('stg_customer') }}
    )
    , stg_person as (
        select
            businessentityid as personid
            , concat(ifnull(firstname,' '),' ',ifnull(middlename,' '),' ',ifnull(lastname,' ')) as fullname
            , case
                when persontype = "SC" then "Store contact"
                when persontype = "IN" then "Individual customer"
                when persontype = "SP" then "Sales person"
                when persontype = "EM" then "Employee"
                when persontype = "VC" then "Vendor contact"
                when persontype = "GC" then "General contact"
            end as persondescription
        from {{ ref('stg_person') }}
    )
    , stg_store as (
        select
            businessentityid as storeid
            , store
        from {{ ref('stg_store') }}
    )
    , stg_personcreditcard as (
        select
            businessentityid as personcreditcardid
            , creditcardid
        from {{ ref('stg_personcreditcard') }}
    )
    , stg_creditcard as (
        select
            creditcardid
            , cardtype
        from {{ ref('stg_creditcard') }}
    )
    , join_person_card as (
        select
            stg_person.personid
            , stg_person.fullname
            , stg_person.persondescription
            , stg_creditcard.cardtype
        from stg_person
        left join stg_personcreditcard on stg_person.personid = stg_personcreditcard.personcreditcardid
        left join stg_creditcard on stg_personcreditcard.creditcardid = stg_creditcard.creditcardid
    )
    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customers_sk  
            , stg_customer.customerid
            , join_person_card.fullname
            , join_person_card.persondescription
            , join_person_card.cardtype
            , stg_store.store
        from stg_customer
        left join join_person_card on stg_customer.personid = join_person_card.personid
        left join stg_store on stg_customer.storeid = stg_store.storeid
    )
select *
from joined