with source as (
    select * from {{ source('gbif', 'silver_occurrences') }}
),
renamed as (
    select
        gbifID                  as occurrence_id,
        species,
        genus,
        family,
        `order`                 as taxon_order,
        class                   as taxon_class,
        kingdom,
        decimalLatitude         as latitude,
        decimalLongitude        as longitude,
        country_clean           as country,
        stateProvince           as state_province,
        grid_cell,
        lat_bin,
        lon_bin,
        hemisphere,
        year,
        month,
        day,
        eventDate               as event_date,
        observer_key            as recorded_by,
        institutionCode         as institution_code,
        basisOfRecord           as basis_of_record,
        occurrenceStatus        as occurrence_status
    from source
),
deduped as (
    select *,
        row_number() over (
            partition by occurrence_id
            order by year desc, month desc
        ) as rn
    from renamed
)
select * except (rn)
from deduped
where rn = 1
