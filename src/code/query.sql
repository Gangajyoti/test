/*
mysql tested
*/
with recursive Date_Ranges AS (
     select '2023-10-30' as Date
    union all
    select date + interval 1 day
    from Date_Ranges
    where date < '2023-11-05')
 select date as redemption_date , case when p.redemptionCount is null then 0
					else  p.redemptionCount end   as  redemption_count
                    from Date_Ranges dg left join
 (select redemptionDate,redemptionCount from (select redemptionDate,redemptionCount,createDateTime,
 dense_rank() over(partition by redemptionDate order by createDateTime desc) as ranked
  from tblRedemptions tred where tred.retailerId = 300 and tred.redemptionDate between '2023-10-30' and '2023-11-05')d where d.ranked =1 )p on dg.Date = p.redemptionDate	order by redemption_count

/*
question 1:  Which date had the least number of redemptions and what was the redemption count?
 => redemption_date = 2023-11-02 , redemption_count = 0

question 2: Which date had the most number of redemptions and what was the redemption count?
=> redemption_date = 2023-11-04 , redemption_count = 5224

question 3: What was the createDateTime for each redemptionCount in questions 1 and 2?
=> createDateTime are null and 2023-11-05 11:00:00 UTC respectively

question 4: Is there another method you can use to pull back the most recent redemption count, by
redemption date, for the date range 2023-10-30 to 2023-11-05, for retailer "ABC Store"?
In words, describe how you would do this (no need to write a query, unless you’d like to).

=> Incremental Data Loading method is also one method we can use for this task.


+----------+---------------+
|      date|redemptionCount|
+----------+---------------+
|2023-10-30|           4274|
|2023-10-31|           5003|
|2023-11-01|           3930|
|2023-11-02|              0|
|2023-11-03|           3810|
|2023-11-04|           5224|
|2023-11-05|           3702|
+----------+---------------+

redemption_date     redemption_count    createDateTime
2023-11-02	        0	                null
2023-11-05	        3702	            2023-11-06 11:00:00 UTC
2023-11-03	        3810	            2023-11-06 11:00:00 UTC
2023-11-01	        3930	            2023-11-06 11:00:00 UTC
2023-10-30	        4274	            2023-11-06 11:00:00 UTC
2023-10-31	        5003	            2023-11-06 11:00:00 UTC
2023-11-04	        5224	            2023-11-05 11:00:00 UTC
*/


