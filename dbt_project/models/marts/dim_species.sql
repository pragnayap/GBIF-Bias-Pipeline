select distinct
    {{ dbt_utils.generate_surrogate_key(['species']) }} as species_key,
    species,
    genus,
    family,
    taxon_order,
    taxon_class,
    kingdom
from {{ ref('stg_occurrences') }}
where species is not null
