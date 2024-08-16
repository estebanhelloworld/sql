---------Modeling and cleaning the data into different tables to optimize query performance----------------
---------Star Schema Generation-------

--DIM view: Region
USE [Practice]
GO

drop view if exists [dbo].[Region]
GO

create view [dbo].[Region]
as
(Select distinct [Postal Code] as Postal_Code, Country, City, State, Region
from [Practice].[dbo].[Superstore]
)
GO

--Dim view: Products
drop view if exists [dbo].[Products]
GO

create view [dbo].[Products]
as
(select distinct [Product ID] as Product_ID, Category, [Sub-Category] as Sub_Category
, [Product Name] as Product_Name
from [Practice].[dbo].[Superstore])
GO

--Dim view: Shipments
drop view if exists [dbo].[Shipping]
GO

create view [dbo].[Shipping]
as
(select distinct [Order ID] as Order_ID
, cast([Order Date] as date) as Order_Date
, cast([Ship Date] as date) as Ship_Date
, datediff(day, [Order Date],[Ship Date]) as Days_to_Ship
, [Ship Mode] as Ship_Mode
from [Practice].[dbo].[Superstore])
GO

-- FACT Table: Orders
drop table if exists [dbo].[Superstoreb]
GO

select *  into [dbo].[Superstoreb]
from [Practice].[dbo].[Superstore]
GO

ALTER TABLE [dbo].[Superstoreb]
drop column Country, City, State, Category, [Sub-Category], [Product Name], Region
, [Order Date], [Ship Date], [Ship Mode], Profit
GO

/*
select top 10 * from [Practice].[dbo].[Region]
select top 10 * from [Practice].[dbo].[Products]
select top 10 * from [Practice].[dbo].[Shipping]
select top 10 * from [Practice].[dbo].[Superstoreb]
*/

---------------------------------------------------------------------------------------------------------
--------------------------Preparing data for Dasbhoard Ingestion-----------------------------------------
drop table if exists [Practice].[dbo].[FirstOrdersProducts];

with 
firstorder as (
select rank() over (partition by a.[Customer ID] order by
b.Order_Date) as Row_Num -- selecting all products of the first order of each client
, a.[Order ID] as Order_ID
, a.[Customer ID] as Customer_ID
, a.[Customer Name] as Customer_Name
, a.Segment
, b.Order_Date
, a.[Postal Code] as Postal_Code
, c.*
, cast(case when a.Sales like '%[A-Za-z]%' then null
	   when a.Sales like '%["/]%' then null
	   when a.Sales not like '%.%' then a.Sales
	   else left(a.Sales,charindex('.',a.Sales)-1)
	   end as int) as Price
, cast(case when a.Quantity like '%[A-Za-z]%' then null
       when a.Quantity like '%["]%' then null
	   when a.Quantity not like '%.%' then a.Quantity
	   else left(a.Quantity,charindex('.',a.Quantity)-1)
	   end as int) as Quantity
, case when a.Discount like '%[A-Za-z]%' then null
       when a.Discount like '%["]%' then null
      when cast(a.Discount as float) >= 0.8 then null
      when cast(a.Discount as float) < 0.8 then cast(a.Discount as float)
	  else a.Discount end 
	  as Discount
from [Practice].[dbo].[Superstoreb] a
left join [Practice].[dbo].[Shipping] b
ON a.[Order ID]=b.Order_ID
left join [Practice].[dbo].[Products] c
ON a.[Product ID]=c.Product_ID
),

finalsale as 
(
select *
, case when isnull(Discount,0) <= 0 then Price*Quantity
       when Discount > 0 then Price*Quantity*(1-Discount) end as Sale_with_Discount   
from firstorder
)

select *
into [Practice].[dbo].[FirstOrdersProducts]
from finalsale
where Row_Num = 1 -- Retrieving only the products for the first order of each client

select * from [Practice].[dbo].[FirstOrdersProducts]

----------------------------------------------------------------------------------------------
-------------------------------------DATA EXPLORATION SCRIPTS---------------------------------

--Pivot example--
drop table if exists temp
select Segment, Category, Sub_Category, [2014], [2015], [2016], [2017]
into temp
from
(
select Segment, Category, Sub_Category, year(Order_Date) as Year_Ordered
, round(Sale_with_Discount,0) as Total_Discounted_Sales
from [Practice].[dbo].[FirstOrdersProducts]
) as sourcetable
PIVOT
(sum(Total_Discounted_Sales)
for Year_Ordered in ([2014], [2015], [2016], [2017])
) as Pivottable
order by 1
,case when Category = 'Furniture' then 1
     when Category = 'Office Supplies' then 2
	 when Category = 'Technology' then 3
	 end asc
, Sub_Category asc  --Table created until this line--

drop table if exists #Temp1  --Replace null values with 0 using variables and a loop statement--
SELECT ID,Col_Names INTO #Temp1 FROM 
                                     (VALUES(1,'2014'),
									        (2,'2015'),
											(3,'2016'),
											(4,'2017')) AS Temp1(ID,Col_Names);

DECLARE @query NVARCHAR(MAX);
DECLARE @z int
set @z = 1

WHILE @z < 5
BEGIN

SET @query ='update temp set ['+(select Col_Names from #Temp1 where ID = @z)+'] = 0 
where ['+(select Col_Names from #Temp1 where ID = @z)+'] is null' 

EXEC Sp_EXECUTESQL @query;

SET @z = @z + 1
END

select * from temp

--Aggregation example--
select
b.State
, a.Category
, round(sum(Sale_with_Discount),0) as Total_Discounted_Sales
from [Practice].[dbo].[FirstOrdersProducts] a
left join [Practice].[dbo].[Region] b
ON a.Postal_Code=b.Postal_Code
group by a.Category, b.State
order by 1,2 
offset 0 rows
