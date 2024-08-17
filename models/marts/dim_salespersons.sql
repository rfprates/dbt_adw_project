with
    stg_salesperson as (
        select 
            businessentityid as salespersonid
            , territoryid
            , salesquota
            , bonus 
            , salesytd
            , saleslastyear
            , commissionpct
        from {{ ref('stg_salesperson') }}
    )
    , stg_employee as (
        select
            businessentityid as employeebusinessentityid
            , jobtitle
        from {{ ref('stg_employee') }}
    )
    , stg_person as (
        select
            businessentityid as personbusinessentityid
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
    , stg_salesterritory as (
        select
            territoryid
            , countryregioncode
            , countryregion
            , territorygroup
        from {{ ref('stg_salesterritory')}}
    )
    , joined_salesperson_territory as (
        select
            stg_salesperson.salespersonid
            , stg_salesperson.salesquota
            , stg_salesperson.bonus
            , stg_salesperson.salesytd
            , stg_salesperson.saleslastyear
            , stg_salesperson.commissionpct
            , stg_salesterritory.countryregioncode
            , stg_salesterritory.countryregion
            , stg_salesterritory.territorygroup
        from stg_salesperson
        left join stg_salesterritory on stg_salesperson.territoryid = stg_salesterritory.territoryid
    )
    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['salespersonid']) }} as salespersons_sk
            , joined_salesperson_territory.salespersonid
            , stg_person.fullname
            , stg_employee.jobtitle
            , stg_person.persondescription
            , joined_salesperson_territory.salesquota
            , joined_salesperson_territory.bonus
            , joined_salesperson_territory.salesytd
            , joined_salesperson_territory.saleslastyear
            , joined_salesperson_territory.commissionpct
            , joined_salesperson_territory.countryregioncode
            , joined_salesperson_territory.countryregion
            , joined_salesperson_territory.territorygroup
        from joined_salesperson_territory
        left join stg_employee on joined_salesperson_territory.salespersonid = stg_employee.employeebusinessentityid
        left join stg_person on stg_employee.employeebusinessentityid = stg_person.personbusinessentityid
    )
select *
from joined