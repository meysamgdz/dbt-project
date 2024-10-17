with departures as (
				select origin as faa,
						count(origin) as nunique_from,
						count(sched_dep_time) as dep_planned,
						sum(cancelled) as dep_cancelled,
						sum(diverted) as dep_diverted,
						count(arr_time) as dep_occ,
						count(distinct tail_number) as dep_tail, -- # of unique airplanes
						count(distinct airline) as dep_nunique_airlines
				from {{ref("prep_flights")}}
				group by origin
				),
arrivals as (
				select dest as faa,
						count(dest) as nunique_to,
						count(sched_arr_time) as arr_planned,
						sum(cancelled) as arr_cancelled,
						sum(diverted) as arr_diverted,
						count(arr_time) as arr_occ,
						count(distinct tail_number) as arr_tail, -- # of unique airplanes
						count(distinct airline) as arr_nunique_airlines
				from {{ref("prep_flights")}}
				group by dest
				),
total_stats as (
				select faa,
						nunique_to,
						nunique_from,
						dep_planned + arr_planned as tot_planned,
						dep_cancelled + arr_cancelled as tot_cancelled,
						dep_diverted + arr_diverted as tot_diverted,
						dep_nunique_airlines + arr_nunique_airlines as tot_nunique_airlines
				from departures
				join arrivals
				using (faa)
)
select city,
		country,
		name,
		total_stats.*
from {{ref("prep_airports")}}
right join total_stats
using (faa)
order by city