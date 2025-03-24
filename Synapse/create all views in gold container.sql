------calendar
create view gold.calendar
AS
SELECT * from 
OPENROWSET
(
    BULK 'https://azuredl2.blob.core.windows.net/silver/AdventureWorks_Calendar',
    format ='parquet'
) as query1;


--- Customers
create view gold.customers
AS
SELECT * from 
OPENROWSET
(
    BULK 'https://azuredl2.blob.core.windows.net/silver/AdventureWorks_Customers',
    format ='parquet'
) as query1

----sub categories

create view gold.products_subcategories
AS
SELECT * from 
OPENROWSET
(
    BULK 'https://azuredl2.blob.core.windows.net/silver/AdventureWorks_Product_Subcategories',
    format ='parquet'
) as query2

----Products
create view gold.products
AS
SELECT * from 
OPENROWSET
(
    BULK 'https://azuredl2.blob.core.windows.net/silver/AdventureWorks_Products',
    format ='parquet'
) as query3

---returns
create view gold.returns_1
AS
SELECT * from 
OPENROWSET
(
    BULK 'https://azuredl2.blob.core.windows.net/silver/AdventureWorks_Returns',
    format ='parquet'
) as query4

----sales
create view gold.sales
AS
SELECT * from 
OPENROWSET
(
    BULK 'https://azuredl2.blob.core.windows.net/silver/AdventureWorks_Sales',
    format ='parquet'
) as query5

