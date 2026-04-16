with cells as (
    select
        grid_cell,
        lat_bin,
        lon_bin,
        first(country,        true)  as country,
        first(state_province, true)  as state_province,
        first(hemisphere,     true)  as hemisphere
    from {{ ref('stg_occurrences') }}
    where grid_cell is not null
    group by grid_cell, lat_bin, lon_bin
),

with_continent as (
    select
        *,
       
        case
            when lat_bin <= -60
                then 'Antarctica'
            when lat_bin between -55 and  -5 and lon_bin between  110 and  180
                then 'Oceania'
            when lat_bin between -60 and  15 and lon_bin between  -85 and  -30
                then 'South America'
            when lat_bin between  15 and  75 and lon_bin between -170 and  -30
                then 'North America'
            when lat_bin between  40 and  75 and lon_bin between  -15 and   40
                then 'Europe'
            when lat_bin between -40 and  40 and lon_bin between  -20 and   50
                then 'Africa'
            when lat_bin between  40 and  45 and lon_bin between   40 and   60
                then 'Asia'
            when lat_bin between   0 and  80 and lon_bin between   40 and  180
                then 'Asia'
            else 'Other'
        end as continent
    from cells
)

select
    {{ dbt_utils.generate_surrogate_key(['grid_cell']) }}  as geo_key,
    grid_cell,
    lat_bin,
    lon_bin,
    country,
    state_province,
    hemisphere,
    continent,
    
    concat(
        cast(abs(lat_bin) as string),
        case when lat_bin >= 0 then 'N' else 'S' end,
        ' / ',
        cast(abs(lon_bin) as string),
        case when lon_bin >= 0 then 'E' else 'W' end
    ) as grid_label
from with_continent