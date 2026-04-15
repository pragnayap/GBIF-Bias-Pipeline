with stg as (
    select * from {{ ref('stg_occurrences') }}
),
dim_sp  as ( select * from {{ ref('dim_species')   }} ),
dim_geo as ( select * from {{ ref('dim_geography') }} ),
dim_dt  as ( select * from {{ ref('dim_date')      }} ),

joined as (
    select
        s.occurrence_id,
        sp.species_key,
        g.geo_key,
        d.date_key,
        s.recorded_by,
        s.institution_code,
        s.basis_of_record,
        s.occurrence_status,
        s.latitude,
        s.longitude,
        s.year,
        s.month,
        1 as observation_count,
        row_number() over (
            partition by s.occurrence_id
            order by s.occurrence_id
        ) as rn
    from stg s
    left join dim_sp  sp on sp.species  = s.species
    left join dim_geo g  on g.grid_cell = s.grid_cell
    left join dim_dt  d  on d.year = s.year and d.month = s.month
    where sp.species_key is not null
      and g.geo_key       is not null
)

select * except (rn)
from joined
where rn = 1
