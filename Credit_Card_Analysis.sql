select * from credit_card_transcations;

--Top 5 cities with highest spends and their percentage contribution of total credit card spends.
with cte as(
select city,sum(amount) as total_spend from credit_card_transcations 
group by city),
cte2 as(
select sum(cast(total_spend as bigint)) as rm from cte),
cte3 as(
select city,total_spend,rm from cte,cte2)
select top 5 city,total_spend,round((100.0*(cast(total_spend as float)/rm)),2) as percentage_contribution from cte3
order by percentage_contribution desc;


--Highest spend month and amount spent in that month for each card type.
with cte as(
select card_type,year(transaction_date) as year_,month(transaction_date) as month_,sum(amount) as total from credit_card_transcations
group by card_type,month(transaction_date),year(transaction_date)),
cte2 as(
select card_type,max(total) as mx from cte
group by card_type)
select o.card_type,year_,month_,o.total
from cte as o
join cte2 as l
on o.card_type=l.card_type
where o.total=l.mx;

--Transaction details of each credit card when the cumulative spend reaches 1,000,000.
with cte as(
select *,sum(amount) over(partition by card_type order by transaction_date,amount) as sm
from credit_card_transcations),
cte2 as(
select *,row_number() over(partition by card_type order by sm) as rn
from cte
where sm>=1000000)
select * from cte2
where rn=1; 

--Max. and Min. spend category for each city.
with cte as(
select city,exp_type,sum(amount) as sm from credit_card_transcations
group by city,exp_type),
cte2 as(
select city,max(sm) as mx from cte
group by city),
cte3 as(
select city,min(sm) as mn from cte
group by city),
cte4 as(
select a.city,a.exp_type as highest_exp_type,a.sm
from cte as a
join cte2 as b
on a.city=b.city
where a.sm=b.mx),
cte5 as(
select d.city,d.exp_type as lowest_exp_type,e.mn
from cte as d
join cte3 as e
on d.city=e.city
where d.sm=e.mn)
select l.city,l.highest_exp_type,m.lowest_exp_type
from cte4 as l
join cte5 as m
on l.city=m.city
order by l.city;

--Female contribution for expense type in percentage.
with cte as(
select exp_type,sum(amount) as s from credit_card_transcations
where gender='F'
group by exp_type),
cte2 as(
select exp_type,sum(amount) as r from credit_card_transcations
group by exp_type),
cte3 as(
select a.exp_type,s,r
from cte as a
join cte2 as b
on a.exp_type=b.exp_type)
select exp_type,((cast(s as float)/r)*100) as contribution from cte3
order by contribution desc;

--Card and expense type combination having highest month over month growth in Jan-2014.
with cte as(
select card_type,exp_type,month(transaction_date) as month_,year(transaction_date) as year_,sum(amount) as total from credit_card_transcations
where (month(transaction_date)=12 and year(transaction_date) = 2013) or (month(transaction_date)=01 and year(transaction_date)=2014)
group by card_type,exp_type,month(transaction_date),year(transaction_date)),
cte2 as(
select *,lag(total,1) over(partition by card_type,exp_type order by year_,month_) as previous from cte),
cte3 as(
select * from cte2
where not previous is null),
cte4 as(
select *,(total-previous) as difference_ from cte3)
select top 1 * from cte4
where difference_>0
order by difference_ desc;

--During weekends city having highest average spend.
with cte as(
select *,datename(weekday,transaction_date) as nm from credit_card_transcations),
cte2 as(
select city,sum(amount) as sm,count(*) as cn from cte
where nm in ('Saturday','Sunday')
group by city)
select top 1 city,(cast(sm as float)/cn) as average from cte2
order by average desc;

--City to reach 500 transactions in minimun days. 
with cte as(
select *,row_number() over(partition by city order by transaction_date,transaction_id) as rn
from credit_card_transcations),
cte2 as(
select *,lag(transaction_date,1) over(partition by city order by rn) as lg from cte
where rn=1 or rn=500),
cte3 as(
select city,DATEDIFF(day,lg,transaction_date) as difference_
from cte2),
cte4 as(
select min(difference_) as mn from cte3)
select a.city,a.difference_
from cte3 as a
join cte4 as b
on a.difference_=b.mn;


























