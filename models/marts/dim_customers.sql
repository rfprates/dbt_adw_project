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
            businessentityid as personbusinessentityid
            , concat(ifnull(firstname,' '),' ',ifnull(middlename,' '),' ',ifnull(lastname,' ')) as fullname
            , persontype
        from {{ ref('stg_person') }}
    )
    , stg_store as (
        select
            businessentityid as storebusinessentityid
            , store
        from {{ ref('stg_store') }}
    )
    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['customerid', 'personid', 'storeid']) }} as customer_sk  
            , stg_customer.customerid
            , stg_person.fullname
            , stg_person.persontype
            , stg_store.store
        from stg_customer
        left join stg_person on stg_customer.personid = stg_person.personbusinessentityid
        left join stg_store on stg_customer.storeid = stg_store.storebusinessentityid
    )
select *
from joined