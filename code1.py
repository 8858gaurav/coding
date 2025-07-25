# !apt-get update 
# !apt-get install openjdk-17-jdk-headless -qq > /dev/null 

# import os 
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64" 
# os.environ["SPARK_HOME"] = "/usr/local/lib/python3.11/dist-packages/pyspark"

from pyspark.sql import SparkSession 

spark = SparkSession.builder \
.appName("PySparkColab") \
.enableHiveSupport() \
.master("local[*]") \
.getOrCreate() 


# Q1. In the below table “Market”, replace 'HONG KONG & TAIWAN' & 'CHINA' to ‘Greater China’ and then combine the tables to get all information 

# together to create the mapping file having Geo - Region - Market all in one table. 

spark.createDataFrame([ {"Geo": "EMEA", "Market": 'GERMANY'}, {"Geo": "JAPAN", "Market": 'JAPAN'}, {"Geo": "APAC", "Market": 'INDIA'}, {"Geo": "APAC", "Market": 'HONG KONG & TAIWAN'}, {"Geo": "EMEA", "Market": 'UNITED KINGDOM'}, {"Geo": "EMEA", "Market": 'BENELUX'}, {"Geo": "EMEA", "Market": 'NORDIC'}, {"Geo": "APAC", "Market": 'KOREA'}, {"Geo": "APAC", "Market": 'ANZ'}, {"Geo": "APAC", "Market": 'SEA'}, {"Geo": "EMEA", "Market": 'RUSSIA & CIS'}, {"Geo": "APAC", "Market": 'CHINA'}, {"Geo": "EMEA", "Market": 'IBERICA'}, {"Geo": "EMEA", "Market": 'MEDITERRANEAN'} 

]).createOrReplaceTempView("df_market") 

spark.createDataFrame([ {"Market_Area": "GERMANY", "Region": 'Central'}, {"Market_Area": "JAPAN", "Region": 'Japan'}, {"Market_Area": "INDIA", "Region": 'IND'}, {"Market_Area": "UNITED KINGDOM", "Region": 'UK'}, {"Market_Area": "BENELUX", "Region": 'Western'}, {"Market_Area": "NORDIC", "Region": 'Western'}, {"Market_Area": "KOREA", "Region": 'Korea'}, {"Market_Area": "ANZ", "Region": 'ANZ'}, {"Market_Area": "SEA", "Region": 'SEA'}, {"Market_Area": "RUSSIA & CIS", "Region": 'Central'}, {"Market_Area": "Greater China", "Region": 'Greater China'}, {"Market_Area": "IBERICA", "Region": 'Western'}, {"Market_Area": "MEDITERRANEAN", "Region": 'Central'}, {"Market_Area": "SSA & ISRAEL", "Region": 'UKI'} 

]).createOrReplaceTempView("df_region") 

spark.sql(""" WITH X AS ( SELECT *, REPLACE(REPLACE(Market, 'CHINA', 'Greater China'), 'HONG KONG & TAIWAN', 'Greater China') AS Market_New FROM df_market ) SELECT X.Geo, X.Market_New, r.Region FROM X INNER JOIN df_region AS r ON X.Market_New = r.Market_Area""").show() 

# Q2. Query the two cities in STATION with the shortest and longest CITY names, as well as their respective lengths (i.e. number of characters in the name). 

# If there is more than one smallest or largest city, choose the one that comes first when ordered alphabetically. 

# Also, find out the stations where the length of the CITY is more than 10 or the STATE has space in between. 

data_station = [ (6, "Dallas","Texas",32.7767,-96.797,"Dallas"), (8, "San Antonio","Texas",29.4241,-98.4936,"San Antonio"), (10, "Philadelphia","Pennsylvania",39.9526,-75.1652,"Philadelphia"), (10, "Ahiladelphia","Pennsylvania",39.9526,-75.1652,"Ahiladelphia"), (1, "New York","New York",40.7128,-74.006,"New York"), (5, "Chicago","Illinois",41.8781,-87.6298,"Chicago"), (3, "Boston","Massachusetts",42.3601,-71.0589,"Boston"), (9, "Saint Louis","Missouri",38.627,-90.1994,"Saint Louis"), (2, "Los Angeles","California",34.0522,-118.2437,"Los Angeles"), (11, "Aiami","Florida",25.7617,-80.1918,"Aiami"), (7, "Atlanta","Georgia",33.749,-84.388,"Atlanta"), (4, "Miami","Florida",25.7617,-80.1918,"Miami")] 

spark.createDataFrame(data_station, "ID Integer, City String, State String, Latitude float, Longitude float, Station String").createOrReplaceTempView("df_station") 

spark.sql(""" 

SELECT 

city, 

CHAR_LENGTH(City) AS city_length 

FROM df_station 

QUALIFY ( 

row_number() OVER (ORDER BY CHAR_LENGTH(City), city) = 1 

OR row_number() OVER (ORDER BY CHAR_LENGTH(City) DESC, city ASC) = 1) 

""").show() 

# Also, find out the stations where the length of the CITY is more than 10 or the STATE has space in between. 

spark.sql(""" SELECT station FROM df_station WHERE LENGTH(City) > 10 OR state like '% %'""").show() 

# Q3.a) Write a query to calculate the median salary of employees in a table. 

employee_data = [ (1, 55000), (2, 62000), (3, 48000), (4, 70000), (5, 50000), (6, 58000), (7, 65000), (8, 53000), (9, 60000), (10, 72000) ] spark.createDataFrame(employee_data, "employee_id Integer, salary Integer").createOrReplaceTempView("df_employee_data") 

spark.sql("""WITH base_data AS ( SELECT employee_id, salary, ROW_NUMBER() OVER (ORDER BY salary) AS rn FROM df_employee_data ), median_row_number AS ( SELECT salary, rn FROM base_data WHERE rn IN ((select max(rn) from base_data) / 2, (select max(rn) from base_data) / 2 + 1) ) SELECT AVG(salary) AS median_salary FROM median_row_number""").show() 

# 3.b) Identify employees who have joined with last 60 days from today’s date 

from datetime import date from pyspark.sql.functions import to_date 

employee_records = [ (1, "Alice Smith", '2025-03-01'), (2, "Bob Johnson", '2025-03-10'), (3, "Charlie Brown", '2025-03-15'), (4, "Diana Miller", '2025-04-01'), (5, "Eve Davis", '2025-04-05'), (6, "Frank White", '2025-04-20'), (7, "Grace Taylor", '2025-05-01'), (8, "Harry Wilson", '2025-05-10'), (9, "Ivy Moore", '2025-05-15'), (10, "Jack Green", '2025-06-01') ] 

spark.createDataFrame(employee_records, "employee_id Integer, employee_name String, Join_date String") \
.withColumn("Join_date", to_date("Join_date", "yyyy-MM-dd")).createOrReplaceTempView("df_emp_records") 

spark.sql("""SELECT employee_id, employee_name FROM df_emp_records WHERE join_date BETWEEN (CURRENT_DATE() - 60) AND CURRENT_DATE()""").show() 

spark.sql("""SELECT employee_id, employee_name FROM df_emp_records WHERE join_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND CURRENT_DATE()""").show() 

# 3.c) Retrieve the name of the manager who supervises the most employees. 

manager_employee_relationships = [ ("Alice Benjamin", 1), ("Alice Benjamin", 5), ("Bob Johnson", 2), ("Bob Johnson", 6), ("Charlie Davis", 3), ("Charlie Davis", 7), ("Diana Miller", 4), ("Diana Miller", 8), ("Charlie Davis", 9), ("Eve Wilson", 10) ] 

spark.createDataFrame(manager_employee_relationships, "manager_name String, employee_id Integer") \
.createOrReplaceTempView("df_manager_employee_relationships") 

spark.sql("""WITH base_data AS ( SELECT manager_name, COUNT() AS num_employees, RANK() OVER (ORDER BY COUNT() DESC) AS rnk FROM df_manager_employee_relationships GROUP BY manager_name ) SELECT manager_name FROM base_data WHERE rnk = 1""").show() 

# 3.d) Write a query to group employees by age ranges (e.g., 20–30, 31–40) and count the number of employees in each group. 

employee_ages_ids = [ (20, 121213), (25, 121214), (30, 121215), (35, 121216), (28, 121217), (40, 121218), (33, 121219), (22, 121220), (29, 121221), (38, 121222) ] 

spark.createDataFrame(employee_ages_ids, "age Integer, emp_id Integer") \
.createOrReplaceTempView("df_employee_ages_ids") 

spark.sql("""WITH base_data AS ( SELECT emp_id, age, CASE WHEN age BETWEEN 20 AND 30 THEN '20-30' WHEN age BETWEEN 30 AND 40 THEN '30-40' WHEN age BETWEEN 40 AND 50 THEN '40-50' WHEN age BETWEEN 50 AND 60 THEN '50-60' WHEN age BETWEEN 60 AND 70 THEN '60-70' ELSE '70+' END AS age_bucket FROM df_employee_ages_ids ) SELECT age_bucket, COUNT(*) AS number_of_employees FROM base_data GROUP BY 1""").show() 

# 3.e)Find the most common age group 

age_group_records = [ ('1-10',), ('10-20',), ('1-10',), ('10-20',), ('20-30',), ('30-40',), ('30-40',), ('20-30',), ('30-40',), ('40-50',), ('10-20',), ('30-40',), ('20-30',), ('20-30',), ('20-30',), ('20-30',), ('30-40',), ('20-30',), ('30-40',), ('1-10',) ] 

spark.createDataFrame(age_group_records, "age_group String") \
.createOrReplaceTempView("df_age_group") 

spark.sql("""WITH base_data AS ( SELECT age_group, COUNT(*) AS rnk FROM df_age_group GROUP BY 1 ) SELECT age_group FROM base_data WHERE rnk = (select max(rnk) from base_data)""").show() 

# 3.f) Write a query to identify the employee(s) whose salary is closest to the average salary of the company. 

employee_salary_records = [ (1, 120000), (2, 125000), (3, 118000), (4, 130000), (5, 122000), (6, 128000), (7, 115000), (8, 135000), (9, 121000), (10, 129000) ] 

spark.createDataFrame(employee_salary_records, "emp_id Integer, Salary Integer") \
.createOrReplaceTempView("df_employee_salary_records") 

spark.sql("""WITH company_avg AS ( SELECT id, salary, average(salary) AS overall_average FROM df_employee_salary_records ), ranking AS SELECT id, RANK() OVER (ORDER BY ABS(salary - overall_average)) AS rnk FROM company_avg SELECT id FROM ranking WHERE rnk = 1""").show() 

# Q4. Suppose, we have a product table as given below : 

data_product = [(1,"North East",139), (2,"Sout",220), (1,"South West",193), (3,"West",195), (5,"West",839), (8,"East",467), (3,"North West",764), (8,"North West",579), (2,"South West",389), (9,"South East",986), (3,"East",285),
(10,"East",0), (11,"East",0)] 

spark.createDataFrame(data_product, "Product_id Integer, Region String, Sales_USD Integer").createOrReplaceTempView("df_product") 

# Write a query To identify products that were sold in all regions 

spark.sql("""select DISTINCT product_id from (select product_id, Region from df_product group by 1, 2 having SUM(Sales_USD) > 0) AS t""").show() 

# To display the cumulative percentage of total sales for each product 

spark.sql("""select DISTINCT product_id , ROUND((sum(Sales_USD) OVER(PARTITION BY product_id) / SUM(Sales_USD) OVER()) * 100, 2) AS perc from df_product""").show() 

# To retrieve the average monthly sales by each product & region combination 

spark.sql("""select product_id, Region, ROUND(AVG(Sales_USD), 2) AS avg_sales from df_product group by 1, 2""").show() 

# To display all products where sales exceeded the average monthly sales of that region 

spark.sql("""select product_id, Region from df_product group by 1, 2 having AVG(Sales_USD) > (select AVG(Sales_USD) from df_product)""").show() 

# Q5) Given a table with MovieName, Language, Genre, RunTime 

# Find the Top 5 Lengthiest Movies By Genre & Language 

data_movie = [ ("Movie N","Hindi","Drama",145), ("Movie P","Spanish","Comedy",97), ("Movie U","English","Action",128), ("Movie K","Hindi","Drama",155), ("Movie T","Hindi","Drama",142), ("Movie J","Spanish","Comedy",98), ("Movie O","English","Action",122), ("Movie L","English","Action",130), ("Movie V","Spanish","Comedy",108), ("Movie C","Hindi","Drama",150), ("Movie F","English","Action",125), ("Movie S","Spanish","Comedy",102), ("Movie H","Hindi","Drama",140), ("Movie M","Spanish","Comedy",105), ("Movie A","English","Action",120), ("Movie G","Spanish","Comedy",100), ("Movie I","English","Action",118), ("Movie Q","Hindi","Drama",148), ("Movie B","Spanish","Comedy",95), ("Movie R","English","Action",132)] 

spark.createDataFrame(data_movie, "MovieName String, language String, genre String, Runtime Integer").createOrReplaceTempView("df_movie") 

spark.sql(""" select MovieName, language, genre from (select *, row_number() over(partition by genre, language order by Runtime Desc) as rnk from df_movie) AS t where rnk <=5 """).show() 

# Q6 Write a query to find out 

# Total sales amount during offer and not during offer 

# Total number of transactions during offer and not during offer 

# Which brand recorded maximum / minimum sales (in amount) during Offer period / non-offer period 

import datetime 

# Dummy data for transactions 

transaction_data = [ (1, datetime.date(2023, 1, 5), 'BrandA', 100, 'Jan'), (2, datetime.date(2023, 1, 15), 'BrandB', 200, 'Jan'), (3, datetime.date(2023, 1, 25), 'BrandA', 300, 'Jan'), (4, datetime.date(2023, 2, 5), 'BrandC', 150, 'Feb'), (5, datetime.date(2023, 2, 20), 'BrandB', 250, 'Feb'), (6, datetime.date(2023, 2, 28), 'BrandA', 180, 'Feb') ] 

# Dummy data for offers (offer period by month) 

offer_data = [ ('Jan', datetime.date(2023, 1, 10), datetime.date(2023, 1, 20)), ('Feb', datetime.date(2023, 2, 15), datetime.date(2023, 2, 25)) ] 

spark.createDataFrame(transaction_data, ["txn_id", "date", "brand", "amount", "month"]).createOrReplaceTempView("transaction_table") 
spark.createDataFrame(offer_data, ["month", "offer_start_date", "offer_end_date"]).createOrReplaceTempView("offer_table") 

# Q 6.1)s: Create base_table using LEFT JOIN 

spark.sql(""" CREATE OR REPLACE TEMP VIEW base_table AS SELECT T.date AS txn_date, T.txn_id AS id, T.brand AS brand, T.amount AS amount, O.offer_start_date AS offer_start_date, O.offer_end_date AS offer_end_date FROM transaction_table AS T LEFT JOIN offer_table AS O ON T.month = O.month """) 

# Q 6.2): Total sales and transaction count during and not during offer 

spark.sql(""" SELECT SUM(CASE WHEN txn_date BETWEEN offer_start_date AND offer_end_date THEN amount ELSE 0 END) AS total_sales_during_offer, SUM(CASE WHEN txn_date NOT BETWEEN offer_start_date AND offer_end_date THEN amount ELSE 0 END) AS total_sales_not_during_offer, COUNT(CASE WHEN txn_date BETWEEN offer_start_date AND offer_end_date THEN id ELSE NULL END) AS total_txn_count_during_offer, COUNT(CASE WHEN txn_date NOT BETWEEN offer_start_date AND offer_end_date THEN id ELSE NULL END) AS total_txn_count_not_during_offer FROM base_table """).show() 

# Q 6.3): Brand with max/min sales during and not during offer 

spark.sql(""" WITH max_and_min_base AS ( SELECT brand, SUM(CASE WHEN txn_date BETWEEN offer_start_date AND offer_end_date THEN amount ELSE 0 END) AS total_sales_during_offer, SUM(CASE WHEN txn_date NOT BETWEEN offer_start_date AND offer_end_date THEN amount ELSE 0 END) AS total_sales_not_during_offer FROM base_table GROUP BY brand ), ranking AS ( SELECT *, RANK() OVER (ORDER BY total_sales_during_offer) AS rnk_offer_min, RANK() OVER (ORDER BY total_sales_during_offer DESC) AS rnk_offer_max, RANK() OVER (ORDER BY total_sales_not_during_offer) AS rnk_not_offer_min, RANK() OVER (ORDER BY total_sales_not_during_offer DESC) AS rnk_not_offer_max FROM max_and_min_base ) SELECT brand, total_sales_during_offer, total_sales_not_during_offer FROM ranking WHERE rnk_offer_min = 1 OR rnk_offer_max = 1 OR rnk_not_offer_min = 1 OR rnk_not_offer_max = 1 """).show() 

# Q7) From the SALARY table below, find the emp whose CTC has increased every year from 2020-2022 

data_emp = [(2020,"Sameer",250000), (2021,"Sameer",300000), (2022,"Sameer",315000), (2020,"Yash",300000), (2021,"Yash",345000), (2022,"Yash",345000), (2020,"Rohit",370000), (2021,"Rohit",400000), (2022,"Rohit",425000)] 

spark.createDataFrame(data_emp, "Year Integer, Emp String, CTC Integer").createOrReplaceTempView("df_emp_new") 

spark.sql(""" WITH Base AS ( SELECT *, LEAD(CTC) OVER (PARTITION BY Emp ORDER BY Year) AS next_year_salary, LEAD(CTC, 2) OVER (PARTITION BY Emp ORDER BY Year) AS next_2_year_salary FROM df_emp_new ) SELECT EMP, CTC, next_year_salary, next_2_year_salary FROM Base WHERE next_2_year_salary > next_year_salary AND next_year_salary > CTC""").show() 

# Q.8) Write a SQL query to find the customers who made their second purchase within 7 days of their first purchase 

data_customer_purchase = [(1, 'Alice Smith', '2025-06-27'), (2, 'Fiona Garcia', '2025-06-10'), (3, 'Oliver Jackson','2025-06-23'), (3,'Oliver Jackson','2025-06-25'), (3,'Oliver Jackson','2025-06-26'), (3,'Oliver Jackson','2025-06-28'), (5,'Laura Wilson','2025-06-15'), (5,'Laura Wilson','2025-06-19'), (5,'Laura Wilson','2025-06-29'), (6,'Ivan Hernandez','2025-06-30') ] 

spark.createDataFrame(data_customer_purchase, "customer_id Integer, Customer_name String, purchase_date String").withColumn("purchase_date", to_date("purchase_date", "yyyy-MM-dd")).createOrReplaceTempView("df_customer_purchase") 

spark.sql(""" select customer_name from (SELECT DISTINCT customer_name, row_number() OVER (PARTITION BY customer_id ORDER BY purchase_date) as odr_rnk, datediff( DAY, purchase_date, lag(purchase_date) OVER (PARTITION BY customer_id ORDER BY purchase_date ASC)) as diff_frm_lst_odr FROM df_customer_purchase) where odr_rnk <= 2 and diff_frm_lst_odr <= 7 """).show() 

# Q9) Count number of products in each category based on its price into 3 categories 

# Categories: 
# Low Price: <100 
# Medium Price: >=100 and <500 
# High Price: >=500 
# Create dummy data 

product_data = [ (1, "Laptop", 800), (2, "Mouse", 25), (3, "Keyboard", 120), (4, "Monitor", 450), (5, "USB Cable", 80), (6, "Smartphone", 600), (7, "Charger", 150), (8, "Headphones", 90), (9, "Desk", 550), (10, "Chair", 400) ] 

spark.createDataFrame(product_data, "product_id INT, product_name STRING, price INT").createOrReplaceTempView("products") 

# SQL query to count products by price category 

spark.sql(""" SELECT CASE WHEN price < 100 THEN 'Low Price' WHEN price >= 100 AND price < 500 THEN 'Medium Price' ELSE 'High Price' END AS Category, COUNT(*) AS Count FROM products GROUP BY 1 """).show() 

from datetime import date 
from pyspark.sql.functions import to_date 

# Q10) An e-commerce company wants to identify the total sales generated by its "Loyal and Exclusive" customers. 
# A customer is considered "Loyal and Exclusive" if they meet two criteria: 
# Criteria 1) They have placed orders in at least two different months during the year 2023 
# Criteria 2) They are the only customers registered from their respective city 
# Write a SQL query to report the sum of total_amount for all "Loyal and Exclusive" customers. Round the final sum to two decimal places. 

data_emp = [(201, "Clara", "Miami"), (202, "Daniel", "Dallas"), (203, "Emily", "Miami"), (204, "Fiona", "Seattle")] 

spark.createDataFrame(data_emp,"Customer_id Integer, name String, city String").createOrReplaceTempView("df_emp") 

data_orders = [ (10, 201, "2023-01-05", 50.00, "Miami"), (11, 201, "2023-02-10", 75.00, "Miami"), (12, 202, "2023-03-15", 120.00, "Miami"), (13, 204, "2023-04-01", 90.00, "Seattle"), (14, 204, "2023-05-20", 110.00, "Seattle"), (15, 204, "2023-05-25", 40.00, "Miami")] 

spark.createDataFrame(data_orders, "order_id Integer, customer_id Integer, order_date string, total_amount float, order_city string").withColumn("order_date", to_date("order_date", "yyyy-MM-dd")).createOrReplaceTempView("df_orders") 

spark.sql(""" WITH loyalAndExclusive AS ( SELECT E.Customer_id, E.name, E.city, O.order_id, O.order_date, O.total_amount FROM df_emp AS E LEFT JOIN df_orders AS O ON E.Customer_id = O.customer_id AND E.city = O.order_city ) SELECT SUM(amount) AS total_amount FROM ( SELECT Customer_id, SUM(total_amount) AS amount FROM ( SELECT Customer_id, MONTH(order_date), COUNT(order_id), SUM(total_amount) AS total_amount FROM loyalAndExclusive WHERE order_id IS NOT NULL GROUP BY 1, 2 ) GROUP BY 1 HAVING COUNT(*) >= 2 ) """).show() 

import duckdb 

def run_sql(sql): return duckdb.query(sql).to_df() 

# Q11) There is a queue of people waiting to board a bus. However, the bus has a weight limit of 1000 kilograms, so there may be some people who cannot board. 

# Write a solution to find the person_name of the last person that can fit on the bus without exceeding the weight limit. 
# The test cases are generated such that the first person does not exceed the weight limit. Note that only one person can board the bus at any given turn. 

data_bus = [(5, 'Alice', 250, 1), (4,'Bob', 175, 5), (3,'Alex', 350, 2), (6,'John Cena', 400, 3), (1,'Winston', 500, 6), (2,'Marie', 200, 4)] 

spark.createDataFrame(data_bus, "person_id Integer, person_name string, weight Integer, turn Integer").createOrReplaceTempView("df_bus") 

spark.sql(""" select person_name, turn from (SELECT person_name, turn, sum(weight) OVER (ORDER BY turn) AS total_weight FROM df_bus) AS t where total_weight <= 1000 ORDER BY turn DESC LIMIT 1 """).show() 

# Q12) Measure percentage of users who activated 'Premium' subscription within 30 days of signup. 

import datetime 

# Create dummy data for Users table with proper datetime.date objects 

users_data = [ (1, datetime.date(2023, 1, 1)), (2, datetime.date(2023, 1, 10)), (3, datetime.date(2023, 2, 1)) ] 

# Create dummy data for Subscriptions table 

subscriptions_data = [ (101, 1, "Premium", datetime.date(2023, 1, 15)), (102, 2, "Basic", datetime.date(2023, 1, 10)), (103, 3, "Premium", datetime.date(2023, 3, 5)) ] 

# Create DataFrames and register as SQL views 

spark.createDataFrame(users_data, ["user_id", "signup_date"]).createOrReplaceTempView("Users") spark.createDataFrame(subscriptions_data, ["subscription_id", "user_id", "plan_type", "activation_date"]).createOrReplaceTempView("Subscriptions") 

# SQL query to calculate the percentage of early 'Premium' activations 

spark.sql(""" SELECT ROUND( IF( (SELECT COUNT() FROM Users) = 0, 0, COUNT(DISTINCT s.user_id) * 100.0 / (SELECT COUNT() FROM Users) ), 2) AS early_premium_activation_percentage FROM Users u INNER JOIN Subscriptions s ON u.user_id = s.user_id WHERE s.plan_type = 'Premium' AND s.activation_date <= DATE_ADD(u.signup_date, 30) """).show() 

from datetime import date 
from pyspark.sql.functions import to_date 

# Q13) A sales team tracks leads and their associated projects. They want to calculate the "Project Conversion Rate" for each lead, 
# defined as the number of 'Completed' projects associated with a lead divided by the total number of projects associated with that lead. 
# If a lead has no associated projects, their conversion rate is 0. Round the conversion rate to two decimal places. 
# Write a solution to find the project_conversion_rate for each lead. Return the result table in any order. 

data_lead = [(101, "Alice", "Qualified"), (102, "Bob", "New"), (103, "Carol", "Contacted"), (104, "David", "Closed")] 

spark.createDataFrame(data_lead, "lead_id Integer, lead_name String, status String").createOrReplaceTempView("df_lead") 

data_projects = [(1, 101, "Completed", "2023-01-15"), (2, 101, "In Progress", "2023-02-01"), (3, 101, "Completed", "2023-03-10"), (4, 102, "Planned", "2023-04-01"), (5, 102, "Cancelled", "2023-05-01"), (6, 103, "Completed", "2023-06-01"), (7, 103, "Completed", "2023-07-01"), (8, 103, "In Progress", "2023-08-01")] 

spark.createDataFrame(data_projects, "project_id Integer, lead_id Integer, project_status string, start_date string").withColumn("order_date", to_date("start_date", "yyyy-MM-dd")).createOrReplaceTempView("df_projects") 

spark.sql(""" WITH project_counts AS ( SELECT L.lead_name, SUM(CASE WHEN P.project_status = 'Completed' THEN 1 ELSE 0 END) AS completed_projects, COUNT(P.project_id) AS total_projects FROM df_lead AS L LEFT JOIN df_projects AS P ON L.lead_id = P.lead_id GROUP BY L.lead_name ) SELECT lead_name, ROUND(COALESCE(completed_projects * 100.0 / total_projects, 0), 2) AS project_conversion_rate FROM project_counts""").show() 

# Q14) Our marketing team wants to identify a very specific type of valuable customer - "Consecutive Spender". A Consecutive Spender is the customer 
# who meets below 2 conditions 
# The customer who placed at least three orders in total 
# Among their orders, there must be at least one instance where they placed two or more orders on consecutive calendar days and both of these consecutive orders had a total_amount greater than $50. 

data_customer_orders = [ (101,1,'2025-01-10',50), (102,2,'2025-01-10',75), (103,1,'2025-01-10',25), (104,3,'2025-01-11',100), (105,2,'2025-01-12',60), (106,1,'2025-01-15',80), (107,4,'2025-01-15',30), (108,3,'2025-01-11',40), (109,5,'2025-01-16',20), (110,2,'2025-01-13',45), (111,3,'2025-01-14',55), (112,1,'2025-01-16',90), (113,6,'2025-01-18',60), (114,6,'2025-01-19',70), (115,6,'2025-01-21',30), (116,7,'2025-01-20',80), (117,7,'2025-01-21',90) ] 

spark.createDataFrame(data_customer_orders, "order_id integer, customer_id integer, order_date string, total_amount integer").withColumn("order_date", to_date("order_date", "yyyy-MM-dd")).createOrReplaceTempView("df_customer_orders") 

spark.sql(""" SELECT DISTINCT customer_id FROM ( SELECT DISTINCT customer_id, COUNT(order_id) OVER (PARTITION BY customer_id) AS total_odr_per_cust, DATEDIFF(DAY, order_date, LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)) AS diff_from_last_day, LAG(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date) + total_amount AS total_of_last_two_consicutive_odr FROM df_customer_orders ) WHERE total_odr_per_cust >= 3 AND diff_from_last_day <= 1 AND total_of_last_two_consicutive_odr > 50 ORDER BY 1 """).show() 

# Q15) Create a flag if a customer has used more than one payment method without 
# using Aggregate Functions and Subqueries. ID Column is unique and hence no need of group by as well. 

payment_data = [(1, 0, 10, 20), (2, 0, 0, 30),  (3, 10, 0, 0), (4, 10, 25, 40) ] 

spark.createDataFrame(payment_data, ["ID", "CardSpend", "NetBankingSpend", "UpiSpend"]).createOrReplaceTempView("payments") 

# SQL query to create multi_method_flag without aggregates or subqueries 

spark.sql(""" SELECT ID, CardSpend, NetBankingSpend, UpiSpend, CASE WHEN (IF(CardSpend > 0, 1, 0) + IF(NetBankingSpend > 0, 1, 0) + IF(UpiSpend > 0, 1, 0)) > 1 THEN 1 ELSE 0 END AS multi_method_flag FROM payments """).show() 

 