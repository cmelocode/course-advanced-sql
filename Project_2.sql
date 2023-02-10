/* To reformat this query, I used the style guide of my current data team,
which has specific rules such as
- first field on same line as select
- leading commas aligned with the "t"
-- keywords aligned left 
-- indented CTEs
-- indented "on" clauses 

*/
-- broke out all subqueries into CTEs
-- used CTE and alias names that are short but descriptive  
with active_preferences as 
(
    select customer_id
         , count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by customer_id
),
chicago as 
(
    select geo_location 
    from vk_data.resources.us_cities 
    where city_name = 'CHICAGO' and state_abbr = 'IL'
),
gary as 
(
    select geo_location
    from vk_data.resources.us_cities 
    where city_name = 'GARY' and state_abbr = 'IN'
)
select customer.first_name || ' ' || customer.last_name as customer_name
     , c_address.customer_city
     , c_address.customer_state
     , active_preferences.food_pref_count
     , (st_distance(cities.geo_location, chicago.geo_location) / 1609)::int as chicago_distance_miles
     , (st_distance(cities.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
from vk_data.customers.customer_address as c_address
join vk_data.customers.customer_data customer
        on c_address.customer_id = customer.customer_id
--changed to inner join 
join vk_data.resources.us_cities cities 
        on UPPER(rtrim(ltrim(c_address.customer_state))) = upper(TRIM(cities.state_abbr))
        and trim(lower(c_address.customer_city)) = trim(lower(cities.city_name))
join active_preferences 
        on customer.customer_id = active_preferences.customer_id
cross join chicago 
cross join gary
--in WHERE clause, changed to filtering on customer_address fields only,
--rather than a mix of customer and cities table fields
--this makes it more clear what we are filtering for 
-- broke long where clause into many lines for clarity 
where (
        (trim(c_address.customer_city) ilike '%concord%' 
        or trim(c_address.customer_city) ilike '%georgetown%' 
        or trim(c_address.customer_city) ilike '%ashland%')
        and c_address.customer_state = 'KY')
        or (
            c_address.customer_state = 'CA' 
            and (trim(c_address.customer_city) ilike '%oakland%' 
            or trim(c_address.customer_city) ilike '%pleasant hill%')
            )
        or (
            c_address.customer_state = 'TX' 
            and (trim(c_address.customer_city) ilike '%arlington%') 
            or trim(c_address.customer_city) ilike '%brownsville%'
            )
