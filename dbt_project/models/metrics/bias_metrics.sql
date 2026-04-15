with obs_by_cell as (
    select
        g.geo_key,
        g.grid_cell,
        g.lat_bin,
        g.lon_bin,
        g.country,
        g.continent,
        g.hemisphere,
        g.grid_label,
        count(f.occurrence_id)          as total_observations,
        count(distinct sp.species)      as unique_species,
        count(distinct f.recorded_by)   as unique_observers,
        min(f.year)                     as first_year,
        max(f.year)                     as last_year
    from {{ ref('fact_observations') }} f
    join {{ ref('dim_geography') }}     g  on g.geo_key      = f.geo_key
    join {{ ref('dim_species') }}       sp on sp.species_key = f.species_key
    join {{ ref('dim_date') }}          d  on d.date_key     = f.date_key
    group by 1,2,3,4,5,6,7,8
),
global_stats as (
    select
        avg(total_observations)    as global_avg,
        stddev(total_observations) as global_std,
        sum(total_observations)    as global_total
    from obs_by_cell
)
select
    o.*,
    round((o.total_observations - g.global_avg) / nullif(g.global_std, 0), 3) as sampling_zscore,
    round(o.total_observations * 100.0 / nullif(g.global_total, 0), 4)        as pct_of_global,
    round(o.total_observations * 1.0   / nullif(o.unique_observers, 0), 2)    as obs_per_observer,
    rank() over (order by o.total_observations asc)                            as undersampled_rank,
    rank() over (order by o.total_observations desc)                           as oversampled_rank,
    rank() over (partition by o.continent order by o.total_observations asc)   as undersampled_rank_in_continent,
    case
        when (o.total_observations - g.global_avg) / nullif(g.global_std, 0) < -1.0 then 'Severely undersampled'
        when (o.total_observations - g.global_avg) / nullif(g.global_std, 0) < -0.5 then 'Moderately undersampled'
        when (o.total_observations - g.global_avg) / nullif(g.global_std, 0) >  1.0 then 'Oversampled'
        else 'Near average'
    end as bias_category,
    g.global_avg,
    g.global_std,
    g.global_total
from obs_by_cell o
cross join global_stats g
