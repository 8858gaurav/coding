from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("PySparkColab") \
.enableHiveSupport() \
.master("local[*]") \
.getOrCreate()

# Q 1) Write the SQL query to get the 4th maximum salary of an employee from a table named employees.

data_emp = [
    ('Rohan Sharma', 85000),
    ('Priya Patel', 92000),
    ('Amit Singh', 78000),
    ('Sneha Reddy', 110000),
    ('Vikram Kumar', 65000),
    ('Anjali Gupta', 95000),
    ('Sandeep Verma', 120000),
    ('Pooja Mehta', 72000),
    ('Aditya Rao', 150000),
    ('Meera Nair', 88000) ]

spark.createDataFrame(data_emp, "emp_name String, salary Integer").createOrReplaceTempView("df_emp") spark.sql("""select * from df_emp""").show()

# Q2) There are 5 IPL teams. You need to find out all the possible combinations of matches. Ensure that no team repeats. Eg : CSK vs GT & GT vs CSK should come just once.

data_team = [
    ("Chennai Super Kings",),
    ("Mumbai Indians",),
    ("Kolkata Knight Riders",),
    ("Royal Challengers Bangalore",),
    ("Sunrisers Hyderabad",),
    ("Delhi Capitals",),
    ("Punjab Kings",),
    ("Rajasthan Royals",),
    ("Lucknow Super Giants",),
    ("Gujarat Titans",) ]

spark.createDataFrame(data_team, "Team String").createOrReplaceTempView("df_team") spark.sql("""select * from df_team""").show()

# Q3) Create a flag if a customer has more than 1 payment method in the below payment_table.

data_fop = [
    (101, 5, 0, 12),
    (102, 0, 8, 3),
    (103, 2, 6, 0),
    (104, 0, 0, 7),
    (105, 9, 0, 0),
    (106, 0, 4, 0),
    (107, 1, 10, 5),
    (108, 0, 0, 0),
    (109, 3, 0, 15),
    (110, 0, 7, 9) ]

spark.createDataFrame(data_fop, "Id Integer, card Integer, NetBanking Integer, UPI Integer").createOrReplaceTempView("df_fop") spark.sql("""select * from df_fop""").show()

from datetime import date from pyspark.sql.functions import to_date

# Q 4) Actual distance from the cumulative Km

data_ckms = [
    ("01-Nov", 50),
    ("02-Nov", 150),
    ("03-Nov", 220),
    ("04-Nov", 300),
    ("05-Nov", 380),
    ("06-Nov", 450),
    ("07-Nov", 510),
    ("08-Nov", 600),
    ("09-Nov", 670),
    ("10-Nov", 750) ]

spark.createDataFrame(data_ckms, "Date String, cumulative_kms Integer").createOrReplaceTempView("df_ckms") spark.sql("""select * from df_ckms""").show()

For each driver what is the maximum time

data_driver = [
    ("01-dec", "a1", "driv1", "usr1", "10:00", "10:20"),
    ("02-dec", "a1", "driv1", "usr1", "07:00", "07:35"),
    ("01-dec", "d1", "driv1", "usr2", "10:50", "11:00"),
    ("01-dec", "b1", "driv2", "usr3", "09:50", "10:00"),
    ("01-dec", "b1", "driv2", "usr3", "12:50", "12:55"),
    ("02-dec", "c2", "driv3", "usr4", "08:15", "08:45"),
    ("03-dec", "e3", "driv1", "usr1", "11:00", "11:30"),
    ("03-dec", "a1", "driv2", "usr5", "14:00", "14:10"),
    ("04-dec", "f4", "driv4", "usr6", "09:00", "09:25"),
    ("04-dec", "g5", "driv3", "usr2", "16:00", "16:40") ]

spark.createDataFrame(data_driver, "Date String,id string, driver_id String, user_id string, start_time string, end_time string").createOrReplaceTempView("df_driver") spark.sql("""select * from df_driver""").show()

# Q 5) Write a SQL query to find all numbers that appear at least three times consecutively.

data_cons = [
    (1,),
    (1,),
    (1,),
    (3,),
    (5,),
    (2,),
    (2,),
    (3,),
    (1,)]

spark.createDataFrame(data_cons, "Id Integer").createOrReplaceTempView("df_cons") spark.sql("""select * from df_cons""").show()

# Q 6) Imagine a system where employees tap “in” and “out” as they enter and leave a hospital. How many people are currently inside the hospital.

data_hos = [
    (1, "in", "2019-12-22 9:00:00"),
    (1, "out", "2019-12-22 9:15:00"),
    (2, "in", "2019-12-22 9:00:00"),
    (2, "out", "2019-12-22 9:15:00"),
    (2, "in", "2019-12-22 9:30:00"),
    (3, "out", "2019-12-22 9:00:00"),
    (3, "in", "2019-12-22 9:15:00"),
    (3, "out", "2019-12-22 9:30:00"),
    (3, "in", "2019-12-22 9:45:00"),
    (4, "in", "2019-12-22 9:45:00"),
    (5, "out", "2019-12-22 9:40:00") ]

spark.createDataFrame(data_hos, "Id Integer, status string, time string").createOrReplaceTempView("df_hos") spark.sql("""select * from df_hos""").show()

# Q 7) Get the student count by Result and Subject

data_stu = [
    ("Ram", "History", 20),
    ("Ram", "Maths", 100),
    ("Ram", "Science", 75),
    ("Shyam", "History", 60),
    ("Shyam", "Maths", 85),
    ("Shyam", "Science", 90),
    ("Geeta", "History", 95),
    ("Geeta", "Maths", 70),
    ("Geeta", "Science", 80),
    ("Ram", "English", 55) ]

spark.createDataFrame(data_stu, "name string, sub string, marks Integer").createOrReplaceTempView("df_stu") spark.sql("""select * from df_stu""").show()

