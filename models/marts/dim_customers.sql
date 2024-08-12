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
            /* Function to concatenate first, middle and lastnames of persons registered on stg_person */
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
            /* Using dbt_utils package to generate surrogate key */    
            , stg_customer.customerid
            , stg_person.personbusinessentityid
            , stg_person.fullname
            , stg_person.persontype
            , stg_store.storebusinessentityid
            , stg_store.store
            /* For every customerid, when there is no customer name related to the customerid, a store name is set */
            , case
                when stg_person.fullname is null
                then stg_store.store
                else stg_person.fullname 
            end as customer_fullname
        from stg_customer
        left join stg_person on stg_customer.personid = stg_person.personbusinessentityid
        left join stg_store on stg_customer.storeid = stg_store.storebusinessentityid
    )
select *
from joined