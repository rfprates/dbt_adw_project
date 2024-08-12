with
    stg_salesperson as (
        select 
            businessentityid as salespersonid
            , salesytd
            , saleslastyear
        from {{ ref('stg_salesperson') }}
    )
    , stg_employee as (
        select
            businessentityid as employeebusinessentityid
            , loginid
        from {{ ref('stg_employee') }}
    )
    , stg_person as (
        select
            businessentityid as personbusinessentityid
            , concat(ifnull(firstname,' '),' ',ifnull(middlename,' '),' ',ifnull(lastname,' ')) as fullname
            , persontype
        from {{ ref('stg_person') }}
    )
    , joined as (
        select
            stg_salesperson.salespersonid
            , stg_salesperson.salesytd
            , stg_salesperson.saleslastyear
            , stg_employee.loginid
            , stg_person.fullname
            , stg_person.persontype
        from stg_salesperson
        left join stg_employee on stg_salesperson.salespersonid = stg_employee.employeebusinessentityid
        left join stg_person on stg_employee.employeebusinessentityid = stg_person.personbusinessentityid
    )
select *
from joined