
-- SELECT schema_name.get_inf_gold_data(1980,2022);

-- Replace "schema_name" with relevant schema_name in your case
-- DDL of tables used 
-- schema_name.usa_inflation
CREATE TABLE schema_name.usa_inflation (
	"Year" int4 NULL,
	"Annual" varchar(50) NULL
);
--  schema_name.us_gold_yearly
CREATE TABLE schema_name.us_gold_yearly (
	"Yearly" int4 NULL,
	"Gold_prices" float4 NULL
);

create or replace function schema_name.Get_inf_gold_data (var_min_year_input int DEFAULT null,var_max_year_input int DEFAULT null) 
returns table (
	year int,
	inflation_percent numeric,
	Dollar numeric,
	gold_price numeric,
	"Gold-Dollar" numeric)
language plpgsql
as $$
#variable_conflict use_column
declare 

var_min_year_inf int; -- min year in the inflation table
var_min_year_gold int; -- min year in the gold prices table

var_min_default_year int; --lowest year possible for analysis
var_max_default_year int; --max year possible for analysis

var_min_year int; -- upper limit of year
var_max_year int; -- lower limit of year

var_Min_year_gold_price int; -- used as refence point at which both gold and dollar values are same

begin
	
raise notice '1';

var_min_year_inf := (select min("Year") from schema_name.usa_inflation);
var_min_year_gold := (select min("Yearly") from schema_name.us_gold_yearly);

var_min_default_year:= (case when var_min_year_inf<=var_min_year_gold then var_min_year_gold else var_min_year_inf end);
var_max_default_year := 2022;

IF var_max_year_input< var_min_year_input THEN 
    RAISE EXCEPTION 'Maximum year should be more than Minimum year'; 
elsif var_max_year_input > var_max_default_year THEN 
	RAISE EXCEPTION 'Maximum year should be less than Maximum year common in both Inflation and Gold tables that is %',var_max_default_year; 
elsif var_min_year_input < var_min_default_year THEN 
	RAISE EXCEPTION 'Minimum year should be more or equal than Minimum year common in both Inflation and Gold tables that is %',var_min_default_year; 
END IF; 


var_max_year := (case when var_max_year_input is null then var_max_default_year else var_max_year_input end);
var_min_year := (case when var_min_year_input is null then var_min_default_year else var_min_year_input end);

raise notice 'var_min_year_inf %',var_min_year_inf;
raise notice 'var_min_year_gold %',var_min_year_gold;
raise notice 'var_min_default_year %',var_min_default_year;
raise notice 'var_max_default_year %',var_max_default_year;
raise notice 'var_max_year %',var_max_year;
raise notice 'var_min_year %',var_min_year;
/*
to calculate multiplier to get inflation adjusted dollar value following formula is used
Dollar * (Average_Inflation)^(number of years)
where
	Average_Inflation (avg_inf) = average inflation from provided variable minimum date till that specific year 
	number of years (year_num) = year number since variable minimum date 
								 if variable minimum date is 1975 then 1975 will be year 1 and 1976 will be 2 and so on
	
For specific year multiplier will be
	multiplier = (Average_Inflation)^(number of years)  
	
we will be calculating multiplier metric for comparison
*/
raise notice '2';
-- fetching Inflation data
create temporary table inf as(
with inflat as(	 -- fetching the filtered relevant columns, cleaning it and renaming columns
	select
		"Year" as infl_year,
		rtrim("Annual", '%')::numeric as us_inflation
	from schema_name.usa_inflation
	where "Year" >= var_min_year and "Year" < var_max_year)
				
, inflat2 as(	-- adding columns avg_inf and year_num to calculate each year multiplier
	select
		infl_year,
		us_inflation as inflation_percent,
		avg(us_inflation) over (order by infl_year asc rows between unbounded preceding and current row) as avg_inf,
		row_number() over (order by infl_year) year_num
	from inflat)
-- calculating each year multiplier
select
	inflat2.infl_year,
	inflat2.inflation_percent,
	inflat2.avg_inf,
	POWER((inflat2.avg_inf / 100)+ 1, year_num) multiplier
from inflat2);

raise notice '3';
-- fetching gold data	
create temporary table gold as
	-- fetching the filtered relevant columns, cleaning it and renaming columns
select
	"Yearly" as gold_year, 
	round("Gold_prices"::numeric,2) as gold_price
from schema_name.us_gold_yearly
where "Yearly" >= var_min_year and "Yearly" < var_max_year;

-- var_Min_year_gold_price variable is used as refence point at which both gold and dollar values are same
var_Min_year_gold_price:= (select gold_price from gold where gold_year = var_min_year)/(select multiplier from inf where infl_year = var_min_year);


raise notice '5';
	RETURN QUERY 
		select
			inf.infl_year as year,
			inf.inflation_percent,
			round(var_Min_year_gold_price * inf.multiplier, 2) as Dollar,
			gold.gold_price,
			gold.gold_price-round(var_Min_year_gold_price * inf.multiplier, 2) as "Gold-Dollar"
		from inf
		left join gold on inf.infl_year = gold.gold_year;

drop table if exists inf;
drop table if exists gold;

END; $$ ;