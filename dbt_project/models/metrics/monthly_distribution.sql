select
    d.year,
    d.month,
    d.month_name,
    d.season,
    g.continent,
    g.country,
    g.hemisphere,
    count(f.occurrence_id)          as observations,
    count(distinct sp.species)      as species_richness,
    count(distinct f.recorded_by)   as active_observers
from {{ ref('fact_observations') }} f
join {{ ref('dim_date') }}          d  on d.date_key     = f.date_key
join {{ ref('dim_geography') }}     g  on g.geo_key      = f.geo_key
join {{ ref('dim_species') }}       sp on sp.species_key = f.species_key
group by 1,2,3,4,5,6,7
order by 1,2,5
