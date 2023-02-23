/*
from query profile, determined that the most expensive operation is the window function needed 
to find the most popular recipe per day - but there doesn't seem to be a better way to do this 
Tried a couple different methods to dedup -- group by vs distinct and distinct seemed to result in the faster result
time. Removed order by in final query to maintain performance as data scales. 
*/
alter session set use_cached_result = false;
with sessions_deduped as
    (
        select distinct
               event_id
             , session_id
             , trim(parse_json(event_details):"event", '""')     as event_type
             , trim(parse_json(event_details):"recipe_id", '""') as recipe_id
             , event_timestamp
        from vk_data.events.website_activity
    )
   , session_aggs     as
    (
        select session_id
             , max(event_timestamp)::date                                      as event_day
             , datediff('seconds', min(event_timestamp), max(event_timestamp)) as session_length
             , count(iff(event_type = 'search', event_id, null))               as count_searches
             , count(iff(event_type = 'view_recipe', event_id, null))          as count_views
        from sessions_deduped
        group by session_id
    )
   , most_viewed      as
    (
        select recipe_id             as most_viewed_recipe_id
             , event_timestamp::date as event_day
             , count(*)              as view_count
        from sessions_deduped
        where recipe_id is not null
        group by recipe_id
               , event_day
            qualify row_number() over (partition by event_day
                --when there is a tie, use recipe_id as tiebreaker - ensures deterministic query 
                order by view_count desc, recipe_id asc) = 1
    )
   , final            as
    (
        select most_viewed.event_day
             , most_viewed.most_viewed_recipe_id
             , count(distinct session_id)             as total_sessions
             , sum(session_length) / total_sessions   as avg_sessions_length
             , sum(count_searches) / sum(count_views) as searches_per_view
        from most_viewed
             left join session_aggs
                       on session_aggs.event_day = most_viewed.event_day
        group by most_viewed.event_day
               , most_viewed.most_viewed_recipe_id
    )
select *
from final --order by event_day;
