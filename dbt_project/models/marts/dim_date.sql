select distinct
    {{ dbt_utils.generate_surrogate_key(['year', 'month']) }} as date_key,
    year,
    month,
    case month
        when  1 then 'January'    when  2 then 'February'
        when  3 then 'March'      when  4 then 'April'
        when  5 then 'May'        when  6 then 'June'
        when  7 then 'July'       when  8 then 'August'
        when  9 then 'September'  when 10 then 'October'
        when 11 then 'November'   when 12 then 'December'
    end as month_name,
    case
        when month in (12, 1, 2) then 'Winter'
        when month in (3,  4, 5) then 'Spring'
        when month in (6,  7, 8) then 'Summer'
        else                          'Fall'
    end as season,
    floor(year / 10) * 10 as decade
from {{ ref('stg_occurrences') }}
where year  is not null
  and month is not null
  and year  between 1900 and 2026
  and month between 1 and 12
