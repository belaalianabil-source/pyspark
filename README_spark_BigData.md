# PySpark Big Data Practice Notebook

> A guided reference for the code in `spark_BigData.ipynb`. The notebook practices common PySpark DataFrame, SQL, aggregation, join, window, UDF, and write operations.

## Contents

- [READ DATA](#read-data)
- [Schema difination](#schema-difination)
- [change type of futer](#change-type-of-futer)
- [select function](#select-function)
- [Alias  is like (as) in sql](#alias-is-like-as-in-sql)
- [FILTER it is (WHERE) in sql](#filter-it-is-where-in-sql)
- [SCENARIO 1](#scenario-1)
- [SCENARIO 2](#scenario-2)
- [SCENARIO 3](#scenario-3)
- [withcolummnRenamed](#withcolummnrenamed)
- [(chane name of the column)](#chane-name-of-the-column)
- [wothColumn](#wothcolumn)
- [add a new column](#add-a-new-column)
- [replace null value with Medium](#replace-null-value-with-medium)
- [replace (Low Fat) with (LF) and Regular wit Rg](#replace-low-fat-with-lf-and-regular-wit-rg)
- [sort() it si orederby](#sort-it-si-orederby)
- [LIMIT function](#limit-function)
- [Drop function](#drop-function)
- [dropDuplicates](#dropduplicates)
- [drop duplicate in one or more](#drop-duplicate-in-one-or-more)
- [remove duplicate like function dropDuplicate()](#remove-duplicate-like-function-dropduplicate)
- [UNION and UNION BY NAME](#union-and-union-by-name)
- [preparin the dataframe](#preparin-the-dataframe)
- [runing by column Name or futer](#runing-by-column-name-or-futer)
- [String Functions](#string-functions)
- [DATE FUNCTIONS](#date-functions)
- [current_date()](#currentdate)
- [date_add()](#dateadd)
- [date_sub()](#datesub)
- [we substract from (curr_date)  7 days](#we-substract-from-currdate-7-days)
- [datediff(after,before) becuse the calculate is before-after](#datediffafterbefore-becuse-the-calculate-is-before-after)
- [date_format() cahnge formate of date](#dateformat-cahnge-formate-of-date)
- [HUNDLING NULLS](#hundling-nulls)
- [drona()](#drona)
- [(any) it's removing any recorde have null on his column](#any-its-removing-any-recorde-have-null-on-his-column)
- [fillna()](#fillna)
- [split and indexing](#split-and-indexing)
- [indexing](#indexing)
- [EXPLODE](#explode)
- [array_contains](#arraycontains)
- [groupBy](#groupby)
- [collect_list](#collectlist)
- [pivot](#pivot)
- [when().otherwise()](#whenotherwise)
- [join](#join)
- [WINDOW FUNCTIONS](#window-functions)
- [ROW_NUMBER()](#rownumber)
- [RANK VS DENSE RANK](#rank-vs-dense-rank)
- [Cumulative Sum](#cumulative-sum)
- [USER DEFINED FUNCTIONS (UDF)](#user-defined-functions-udf)
- [DATA WRITING](#data-writing)
- [csv](#csv)
- [append](#append)
- [overwrite](#overwrite)
- [error](#error)
- [ignore](#ignore)
- [parquet](#parquet)
- [table](#table)
- [SPARK SQL](#spark-sql)
- [createTempView](#createtempview)

## Before you run it

- Run this notebook in an environment with PySpark and a configured `spark` session (for example, Databricks if you want to use `.display()` and `/FileStore/...` paths).
- Update the CSV path in the first code cell to a path that exists in your environment.
- Several cells rely on column names such as `Item_Identifier`, `Item_Weight`, and `Outlet_Location_Type`. Ensure the CSV headers and schema use exactly those names.
- Some examples intentionally demonstrate alternative approaches or contain issues noted below. Notebook cells are stateful: later cells may use a variable that was created or changed in an earlier cell.


## Cell 1 — READ DATA

**Code**

```python
file_location = "/Volumes/workspace/default/sample_data/big_mar_seles.csv"
file_type = "csv"
df = spark.read.format(file_type) \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load(file_location)
```

**Explanation**

Loads the CSV file into a Spark DataFrame named `df`. `file_location` is the source path, `format(file_type)` selects CSV, `header=true` treats the first row as column names, and `inferSchema=true` asks Spark to infer data types. The backslashes continue the statement across lines.


## Cell 2 — READ DATA

**Code**

```python
df.display()
```

**Explanation**

Displays the rows and columns of `df` in notebook environments that support `.display()`.


## Cell 3 — READ DATA

**Code**

```python
df.columns
```

**Explanation**

Returns the list of column names in `df`.


## Cell 5 — Schema difination

**Code**

```python
df.printSchema()
```

**Explanation**

Prints the DataFrame schema: each column name and its Spark data type.


## Cell 8 — change type of futer

**Code**

```python
my_ddl_schema = '''  
                    tem_Identifier string,
                    Item_Weight string,
                    Item_Fat_Content string,
                    Item_Visibility double,
                    Item_Type string,
                    Item_MRP double,
                    Outlet_Identifier string,
                    Outlet_Establishment_Year integer,
                    Outlet_Size string,
                    Outlet_Location_Type string,
                    Outlet_Type string,
                    Item_Outlet_Sales double'''
```

**Explanation**

Defines a schema using a DDL string. Each entry gives a column name and type. Note: `tem_Identifier` appears to be a typo for `Item_Identifier`; make sure the schema matches the actual CSV header.


## Cell 9 — change type of futer

**Code**

```python
df = spark.read.format(file_type).schema(my_ddl_schema).option("header", "true") \
  .option("inferSchema", "true") \
  .load(file_location)
```

**Explanation**

Reads the CSV again, applying the schema defined in `my_ddl_schema`. Since a schema is supplied, `inferSchema` is generally unnecessary and does not override explicitly declared types.


## Cell 10 — change type of futer

**Code**

```python
df.printSchema()
```

**Explanation**

Prints the schema after loading with the explicit DDL schema.


## Cell 11 — change type of futer

**Code**

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

**Explanation**

Imports Spark SQL functions such as `col`, `when`, `sum`, and `split`, plus Spark data types such as `StringType` and `StructType`. For production code, importing only the functions you need is clearer.


## Cell 12 — change type of futer

**Code**

```python
my_strct_shema = StructType([
  StructField("tem_Identifier", StringType(), True),
  StructField("Item_Weight", StringType(), True),
  StructField("Item_Fat_Content", StringType(), True),
  StructField("Item_Visibility", StringType(), True),
  StructField("Item_Type", StringType(), True),
  StructField("Item_MRP", StringType(), True),
  StructField("Outlet_Identifier", StringType(), True),
  StructField("Outlet_Establishment_Year", StringType(), True),
  StructField("Outlet_Size", StringType(), True),
  StructField("Outlet_Location_Type", StringType(), True),
  StructField("Outlet_Type", StringType(), True),
  StructField("Item_Outlet_Sales", StringType(), True)
])

df = spark.read.format(file_type).schema(my_strct_shema).option("header", "true") \
  .option("inferSchema", "true") \
  .load(file_location) 
df.printSchema()
```

**Explanation**

Builds a schema programmatically with `StructType` and `StructField`. The final `True` means the field is nullable. All columns are declared strings here. The schema repeats the likely typo `tem_Identifier`; also, `inferSchema=true` does not replace the supplied schema.


## Cell 14 — select function

**Code**

```python
df_select = df.select("Item_Identifier", "Item_Weight", "Item_Fat_Content", "Item_Visibility")


df_select.display()
```

**Explanation**

Selects four columns and stores the result in `df_select`, then displays it. The selected `Item_Identifier` name must match the actual DataFrame schema.


## Cell 16 — Alias  is like (as) in sql

**Code**

```python
df.select(col('Item_Identifier').alias('Item_ID')).display()
```

**Explanation**

Selects `Item_Identifier` and gives it the output name `Item_ID` using `alias()`—similar to `AS` in SQL.


## Cell 19 — SCENARIO 1

**Code**

```python
df.display()
```

**Explanation**

Displays the complete current DataFrame before demonstrating filters.


## Cell 20 — SCENARIO 1

**Code**

```python
df.filter(col('Item_Weight') >21)
```

**Explanation**

Filters rows where `Item_Weight` is greater than 21. It creates a filtered DataFrame but does not call `.display()`, so the notebook may not show a table automatically. Ensure `Item_Weight` is numeric, not a string.


## Cell 23 — SCENARIO 2

**Code**

```python
df.filter((col('Item_Weight') >21) & (col('Item_Fat_Content') == 'LF')).display()
```

**Explanation**

Keeps rows where weight is above 21 **and** fat content equals `LF`. `&` combines Spark boolean conditions; each condition is wrapped in parentheses.


## Cell 25 — SCENARIO 3

**Code**

```python
df2 = df.filter((col('Outlet_Size').isNull()) & (col('outlet_location_type').isin('Tier 1', 'Tier 2')))

df2.select('Outlet_Size','Outlet_Location_Type').display()
```

**Explanation**

Filters rows where `Outlet_Size` is null and outlet location is either `Tier 1` or `Tier 2`, then displays those two columns. Use consistent column capitalization and verify that the actual column is `Outlet_Location_Type`.


## Cell 28 — (chane name of the column)

**Code**

```python
df.withColumnRenamed('Item_Identifier','Item_ID').display()
```

**Explanation**

Renames `Item_Identifier` to `Item_ID` in the returned DataFrame and displays it. Because the result is not assigned back to `df`, the original `df` remains unchanged.


## Cell 31 — add a new column

**Code**

```python
df = df.withColumn('flag',lit('new'))
df.display()
```

**Explanation**

Adds a column named `flag` containing the literal text `new` for every row. `lit()` turns a Python value into a Spark column expression; assigning the result back to `df` updates the variable.


## Cell 32 — add a new column

**Code**

```python
df = df.withColumn('multiply',col('Item_Weight') * col('Item_MRP'))

df.display()
```

**Explanation**

Adds `multiply`, calculated as `Item_Weight * Item_MRP`. Both columns should be numeric for multiplication to work.


## Cell 34 — replace null value with Medium

**Code**

```python
df = df.withColumn('Outlet_Size',when(col('Outlet_Size').isNull(),'Medium'))

df.display()
```

**Explanation**

Attempts to replace null `Outlet_Size` values with `Medium`. Important: `when(condition, value)` without `.otherwise(...)` returns null for rows that do not match, so this expression can overwrite non-null sizes with null. Use `when(col('Outlet_Size').isNull(), 'Medium').otherwise(col('Outlet_Size'))` instead.


## Cell 36 — replace (Low Fat) with (LF) and Regular wit Rg

**Code**

```python
df = df. withColumn('Item_Fat_Content',regexp_replace('Item_Fat_Content','Low Fat','LF')) \
        .withColumn('Item_Fat_Content',regexp_replace('Item_Fat_Content', 'Regular', 'Rg'))

df.display()
```

**Explanation**

Standardizes values in `Item_Fat_Content`: replaces `Low Fat` with `LF`, then `Regular` with `Rg`. `regexp_replace()` performs text replacement. Check the original dataset's labels before choosing replacement values.


## Cell 38 — replace (Low Fat) with (LF) and Regular wit Rg

**Code**

```python
df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))

df.printSchema()
```

**Explanation**

Casts `Item_Weight` to a string and prints the schema. This demonstrates `cast()`, but it changes the column to text rather than a numeric type; use `DoubleType()` if you need numeric calculations.


## Cell 40 — sort() it si orederby

**Code**

```python
df.sort(col('Item_weight').asc()).display()
```

**Explanation**

Sorts rows by `Item_Weight` in ascending order (smallest first). Column spelling/case must match the DataFrame schema; numeric sorting requires a numeric column.


## Cell 41 — sort() it si orederby

**Code**

```python
df.sort(col('Item_weight').desc()).display()
```

**Explanation**

Sorts rows by `Item_Weight` in descending order (largest first).


## Cell 43 — sort() it si orederby

**Code**

```python
df.sort(['Item_Weight','Item_Visibility'],ascending=[False,False]).display()
```

**Explanation**

Sorts by `Item_Weight` and then `Item_Visibility`, both descending. The first sort key has priority; the second breaks ties.


## Cell 44 — sort() it si orederby

**Code**

```python
df.sort(['Item_Weight','Item_Visibility'], ascending=[True,False]).display()
```

**Explanation**

Sorts `Item_Weight` ascending and `Item_Visibility` descending.


## Cell 46 — LIMIT function

**Code**

```python
df.limit(10).display()
```

**Explanation**

Returns and displays at most the first 10 rows. Without an explicit sort, these are not guaranteed to be a meaningful or stable sample.


## Cell 48 — Drop function

**Code**

```python
df.drop('Item_Weight').display()
```

**Explanation**

Returns a DataFrame without `Item_Weight` and displays it. The original `df` is unchanged unless the result is assigned back.


## Cell 49 — Drop function

**Code**

```python
df.drop('Item_Fat_Content','Item_Visibility','Item_Type').display()
```

**Explanation**

Drops three columns—`Item_Fat_Content`, `Item_Visibility`, and `Item_Type`—from the returned DataFrame.


## Cell 51 — dropDuplicates

**Code**

```python
df.dropDuplicates().display()
```

**Explanation**

Removes rows that are exact duplicates across all columns.


## Cell 53 — drop duplicate in one or more

**Code**

```python
df.drop_duplicates(subset=['Item_Weight']).display()
```

**Explanation**

Drops rows duplicated according to `Item_Weight`, keeping one row per distinct weight. This can discard otherwise different records that happen to share a weight.


## Cell 55 — remove duplicate like function dropDuplicate()

**Code**

```python
df.select("Item_Weight").distinct().display()
```

**Explanation**

Selects `Item_Weight` and returns distinct values, removing duplicates in that one-column result.


## Cell 58 — preparin the dataframe

**Code**

```python
data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)
```

**Explanation**

Creates two small example DataFrames (`df1` and `df2`) from Python lists and explicit schemas. Each row contains an ID and a name; this data is used to demonstrate unions.


## Cell 59 — preparin the dataframe

**Code**

```python
df1.display()
```

**Explanation**

Displays the first sample DataFrame.


## Cell 60 — preparin the dataframe

**Code**

```python
df2.display()
```

**Explanation**

Displays the second sample DataFrame.


## Cell 61 — preparin the dataframe

**Code**

```python
df1.union(df2).display()
```

**Explanation**

Combines the rows of `df1` and `df2` using `union()`. Columns are matched by position, not by name, and both DataFrames must have compatible column counts and types. `union()` does not automatically remove duplicates.


## Cell 62 — preparin the dataframe

**Code**

```python
data2 = [('rahul','3'),
        ('jas','4')]
schema2 = 'name STRING, id STRING' 

df2 = spark.createDataFrame(data2,schema2)
```

**Explanation**

Rebuilds `df2` with the columns in the order `name`, then `id`. This deliberately changes the order to demonstrate why `unionByName()` can be safer than `union()`.


## Cell 64 — runing by column Name or futer

**Code**

```python
df1.unionByName(df2).display()
```

**Explanation**

Combines `df1` and `df2` by matching column names instead of positions. Both DataFrames need matching column names and compatible types.


## Cell 67 — String Functions

**Code**

```python
df.select(initcap('Item_Type')).display()
```

**Explanation**

Applies `initcap()` to `Item_Type`, capitalizing the first letter of each word and lowercasing the remaining letters.


## Cell 68 — String Functions

**Code**

```python
df.select(lower('Item_Type')).display()
```

**Explanation**

Converts `Item_Type` values to lowercase.


## Cell 69 — String Functions

**Code**

```python
df.select(upper('Item_Type')).display()
```

**Explanation**

Converts `Item_Type` values to uppercase.


## Cell 70 — String Functions

**Code**

```python
df.select(lower('Item_Type').alias('upper_item_type')).display()
```

**Explanation**

Converts `Item_Type` to lowercase and labels the result `upper_item_type`. The alias name is misleading because the values are lowercase; consider naming it `lower_item_type`.


## Cell 73 — current_date()

**Code**

```python
df=df.withColumn('Curr_Date',current_date())
df.display()
```

**Explanation**

Adds a `Curr_Date` column containing the current date at Spark execution time.


## Cell 76 — date_add()

**Code**

```python
df = df.withColumn('week_after',date_add('Curr_date',7))
df.display()
```

**Explanation**

Adds `week_after`, the date seven days after `Curr_Date`, using `date_add()`.


## Cell 79 — we substract from (curr_date)  7 days

**Code**

```python
df = df.withColumn('week_before',date_sub('Curr_date',7))
df.display()
```

**Explanation**

Adds `week_before`, the date seven days before `Curr_Date`, using `date_sub()`.


## Cell 81 — datediff(after,before) becuse the calculate is before-after

**Code**

```python
df = df.withColumn('datediff',datediff('week_after','Curr_Date'))
df.display()
```

**Explanation**

Adds `datediff`, the number of days between `week_after` and `Curr_Date`. `datediff(end, start)` returns end minus start, so this should be 7.


## Cell 83 — date_format() cahnge formate of date

**Code**

```python
df = df. withColumn('date_format',date_format('week_after','dd-MM-yyyy'))
df.display()
```

**Explanation**

Adds a string-formatted date column in `dd-MM-yyyy` format, for example `14-10-2026`. The output is text, not a date type.


## Cell 87 — drona()

**Code**

```python
df.dropna('all').display()
```

**Explanation**

Removes rows only when **all** columns are null. Rows with at least one non-null value remain.


## Cell 89 — (any) it's removing any recorde have null on his column

**Code**

```python
df.dropna('any').display()
```

**Explanation**

Removes any row that has at least one null in any column. This can remove many records if any field is often missing.


## Cell 90 — (any) it's removing any recorde have null on his column

**Code**

```python
df.dropna(subset='Item_Weight').display()
```

**Explanation**

Removes rows where `Item_Weight` is null. This is useful when that particular column is required.


## Cell 92 — fillna()

**Code**

```python
df.fillna('notavaliable').display()
```

**Explanation**

Fills nulls with the string `notavaliable` (likely intended as `not available`). Spark can only fill compatible column types; string values affect string columns.


## Cell 93 — fillna()

**Code**

```python
df.fillna(0).display()
```

**Explanation**

Fills nulls with numeric zero in compatible columns. Zero is not always a valid substitute for missing data, so choose this carefully.


## Cell 94 — fillna()

**Code**

```python
df.fillna(0,subset='Item_Weight').display()
```

**Explanation**

Fills nulls in `Item_Weight` with zero only. Ensure the column is numeric or use a type-compatible replacement.


## Cell 96 — split and indexing

**Code**

```python
df2 = df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df2.display()
```

**Explanation**

Splits `Outlet_Type` on spaces and stores the pieces as an array in a new version of the column. The result is assigned to `df2`, not `df`.


## Cell 97 — split and indexing

**Code**

```python
df.select(split('Item_Type',' ')).display()
```

**Explanation**

Splits `Item_Type` on spaces and displays the resulting array. The original DataFrame is not changed.


## Cell 99 — indexing

**Code**

```python
df.select(split('Item_Type',' ')[0]).display()
```

**Explanation**

Splits `Item_Type` and selects element `[0]`, the first token. Spark array indexing is zero-based.


## Cell 101 — EXPLODE

**Code**

```python
df2.withColumn('Outlet_Type',explode('Outlet_Type')).display()
```

**Explanation**

Uses `explode()` to turn each element in the `Outlet_Type` array into a separate row. Rows with multiple array elements produce multiple output rows.


## Cell 103 — array_contains

**Code**

```python
df2.withColumn('Type_Flag',array_contains('Outlet_Type','Type1')).display()
```

**Explanation**

Adds `Type_Flag`, a boolean indicating whether the `Outlet_Type` array contains the exact string `Type1`.


## Cell 105 — groupBy

**Code**

```python
df.groupBy("Item_Type").agg(sum('Item_Outlet_Sales')).display()
```

**Explanation**

Groups rows by `Item_Type` and calculates the sum of `Item_Outlet_Sales` for each item category.


## Cell 106 — groupBy

**Code**

```python
df.groupBy('Item_Type').count().display()
```

**Explanation**

Groups rows by `Item_Type` and counts records in each category.


## Cell 107 — groupBy

**Code**

```python
df.groupBy('Item_Type').agg(count('*').alias('count_of_types')).display()
```

**Explanation**

Uses `count('*')` to count rows in each `Item_Type` group and names the result `count_of_types`. This is similar to the previous cell.


## Cell 108 — groupBy

**Code**

```python
df.groupBy('Item_Type').agg(avg('Item_Outlet_Sales').alias('avg_sales')).display()
```

**Explanation**

Calculates average `Item_Outlet_Sales` per `Item_Type` and names the aggregate `avg_sales`.


## Cell 109 — groupBy

**Code**

```python
df.groupBy('Item_Type','Outlet_Size').agg(sum("Item_MRP").alias('total_MRP')).display()
```

**Explanation**

Groups by both `Item_Type` and `Outlet_Size`, then sums `Item_MRP` for each combination. The sum is named `total_MRP`.


## Cell 110 — groupBy

**Code**

```python
df.groupBy('Item_Type','Outlet_Size').agg(sum("Item_MRP").alias('Totale_MRP'),avg('Item_MRP').alias('avg_MRP')).display()
```

**Explanation**

For each `Item_Type` and `Outlet_Size` combination, calculates both total and average `Item_MRP`, naming the results `Totale_MRP` and `avg_MRP`.


## Cell 112 — collect_list

**Code**

```python
data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()
```

**Explanation**

Creates and displays a tiny sample DataFrame mapping users to books. It is used to demonstrate collecting grouped values into arrays.


## Cell 113 — collect_list

**Code**

```python
df_book.groupBy('user').agg(collect_list('book').alias('books')).display()
```

**Explanation**

Groups rows by user and collects each user's book values into an array named `books`. `collect_list()` preserves duplicates if present; it does not guarantee ordering.


## Cell 115 — pivot

**Code**

```python
df3 = df.select('Item_Type','Outlet_Size','Item_MRP')
df3.display()
```

**Explanation**

Creates `df3` with only `Item_Type`, `Outlet_Size`, and `Item_MRP`, then displays this smaller DataFrame for the pivot example.


## Cell 116 — pivot

**Code**

```python
df3.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()
```

**Explanation**

Groups by `Item_Type`, pivots distinct `Outlet_Size` values into separate columns, and calculates average `Item_MRP` in each pivot cell.


## Cell 118 — when().otherwise()

**Code**

```python
df.display()
```

**Explanation**

Displays the current DataFrame as a reference before applying conditional labels.


## Cell 119 — when().otherwise()

**Code**

```python
df_f = df.withColumn('veg_flag',when(col('Item_Type')=='Meat','non-veg').otherwise('veg'))
df_f.display()
```

**Explanation**

Creates `veg_flag`: rows with `Item_Type == 'Meat'` are labeled `non-veg`; all other item types are labeled `veg`. This is a simplified demonstration, not a reliable food-classification rule for every category.


## Cell 120 — when().otherwise()

**Code**

```python
df_v = df_f.withColumn('veg_exp_flag', when((col('veg_flag') =='veg') & (col('Item_MRP') > 100),'veg-expensive')\
                              .when((col('veg_flag') =='veg') & (col('Item_MRP') <= 100),'veg-cheap')\
                              .when((col('veg_flag') =='non-veg') & (col('Item_MRP') > 100),'non-veg-expensive')\
                              .when((col('veg_flag') =='non-veg') & (col('Item_MRP') <= 100),'non-veg-cheap'))
df_v.display()
```

**Explanation**

Adds `veg_exp_flag` using chained `when()` conditions to classify rows by the `veg_flag` and whether `Item_MRP` is above 100. The four labels are veg-expensive, veg-cheap, non-veg-expensive, and non-veg-cheap. Rows not matching any condition become null.


## Cell 122 — join

**Code**

```python
dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)
```

**Explanation**

Creates two example DataFrames: employees (`emp_id`, `emp_name`, `dept_id`) and departments (`dept_id`, `department`). They are sample data for join demonstrations.


## Cell 123 — join

**Code**

```python
df1.display()
df2.display()
```

**Explanation**

Displays both sample DataFrames so their keys and values can be compared before joining.


## Cell 124 — join

**Code**

```python
df1.join(df2,df1['dept_id'] == df2['dept_id'],'inner').display()
```

**Explanation**

Performs an inner join on `dept_id`: only employees whose department ID exists in both DataFrames are returned.


## Cell 125 — join

**Code**

```python
df1.join(df2,df1['dept_id'] == df2['dept_id'],'left').display()
```

**Explanation**

Performs a left join: all employees are kept, and matching department information is added; unmatched employees have null department fields.


## Cell 126 — join

**Code**

```python
df1.join(df2,df1['dept_id'] == df2['dept_id'],'right').display()
```

**Explanation**

Performs a right join: all departments are kept, including departments with no employee; unmatched employee fields are null.


## Cell 127 — join

**Code**

```python
df1.join(df2,df1['dept_id'] == df2['dept_id'],'anti').display()
```

**Explanation**

Performs a left anti join: returns employees whose `dept_id` has no match in the department DataFrame. This can identify missing department references.


## Cell 130 — ROW_NUMBER()

**Code**

```python
from pyspark.sql.window import Window
```

**Explanation**

Imports `Window`, which defines the row groups and ordering used by Spark window functions.


## Cell 131 — ROW_NUMBER()

**Code**

```python
df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()
```

**Explanation**

Adds `rowCol`, a sequential row number based on ascending `Item_Identifier`. If identifiers tie, ordering among tied rows is not guaranteed unless another sort key is added.


## Cell 133 — RANK VS DENSE RANK

**Code**

```python
df1 = df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
        .withColumn('denseRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc())))
df1.display()
```

**Explanation**

Adds `rank` and `denseRank` based on descending `Item_Identifier`. `rank()` leaves gaps after ties (1, 1, 3); `dense_rank()` does not (1, 1, 2).


## Cell 134 — RANK VS DENSE RANK

**Code**

```python
df.withColumn('dum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()
```

**Explanation**

Adds a running sum of `Item_MRP` ordered by `Item_Identifier`, from the first row through the current row. The explicit row frame defines the cumulative window.


## Cell 136 — Cumulative Sum

**Code**

```python
df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()
```

**Explanation**

Adds a cumulative-style sum of `Item_MRP` ordered by `Item_Type`. With the default window frame, rows with the same ordering value can share the same cumulative result; use an explicit `rowsBetween()` frame for row-by-row accumulation.


## Cell 137 — Cumulative Sum

**Code**

```python
df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()
```

**Explanation**

Adds a running sum of `Item_MRP` ordered by `Item_Type`, using an explicit frame from the start through the current row.


## Cell 138 — Cumulative Sum

**Code**

```python
df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()
```

**Explanation**

Adds `totalsum`, the sum of `Item_MRP` over the entire ordered window, from the unbounded preceding row to the unbounded following row. The total is repeated on every row.


## Cell 140 — USER DEFINED FUNCTIONS (UDF)

**Code**

```python
def my_func(x):
    return x*x
```

**Explanation**

Defines a normal Python function that returns the square of its input (`x * x`).


## Cell 141 — USER DEFINED FUNCTIONS (UDF)

**Code**

```python
my_udf = udf(my_func)
```

**Explanation**

Wraps `my_func` as a Spark UDF so it can be applied to Spark DataFrame columns. In robust code, specify the return type, such as `udf(my_func, DoubleType())`.


## Cell 142 — USER DEFINED FUNCTIONS (UDF)

**Code**

```python
df.withColumn('mynewcol',my_udf('Item_MRP')).display()
```

**Explanation**

Applies the UDF to `Item_MRP` and puts the squared result in a new column called `mynewcol`. UDFs can be slower than built-in Spark expressions.


## Cell 145 — csv

**Code**

```python
df.write.format('csv')\
        .save('/FileStore/tables/CSV/data.csv')
```

**Explanation**

Writes `df` in CSV format to the specified path. Spark usually writes a directory containing part files, not one single file. The target path must be valid for the notebook/runtime environment.


## Cell 147 — append

**Code**

```python
df.write.format('csv')\
        .mode('append')\
        .save('/FileStore/tables/CSV/data.csv')
```

**Explanation**

Appends CSV output to the specified path. Append support and behavior depend on the output source and runtime; avoid rerunning accidentally if duplicate data is undesirable.


## Cell 148 — append

**Code**

```python
df.write.format('csv')\
        .mode('append')\
        .option('path','/FileStore/tables/CSV/data.csv')\
        .save()
```

**Explanation**

Also writes CSV in append mode using the `path` option. The path is provided through an option and `.save()` has no argument. This is an alternative syntax to the previous example.


## Cell 150 — overwrite

**Code**

```python
df.write.format('csv')\
.mode('overwrite')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()
```

**Explanation**

Writes CSV using overwrite mode, replacing existing output at the destination. Use carefully because existing data may be replaced.


## Cell 152 — error

**Code**

```python
df.write.format('csv')\
.mode('error')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()
```

**Explanation**

Uses error-if-exists behavior: Spark raises an error if the destination already exists. This is the default save mode in many Spark writers.


## Cell 154 — ignore

**Code**

```python
df.write.format('csv')\
.mode('ignore')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()
```

**Explanation**

Uses ignore mode: if the destination already exists, Spark skips writing instead of failing or overwriting.


## Cell 156 — parquet

**Code**

```python
df.write.format('parquet')\
.mode('overwrite')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()
```

**Explanation**

Writes Parquet with overwrite mode. Parquet is a columnar format commonly used for analytics. The path ends in `.csv` despite the format being Parquet; use a directory/path such as `/FileStore/tables/parquet_data` to avoid confusion.


## Cell 158 — table

**Code**

```python
df.write.format('parquet')\
.mode('overwrite')\
.saveAsTable('my_table')
df.display()
```

**Explanation**

Writes the DataFrame as a managed or catalog table named `my_table` in Parquet format, replacing the table if it already exists, then displays `df`. Table availability depends on the Spark catalog/metastore configuration.


## Cell 161 — createTempView

**Code**

```python
df.createTempView('my_view')
```

**Explanation**

Creates a temporary SQL view named `my_view` from `df`. It is available to SQL queries within the current Spark session and is not a permanent table.


## Cell 162 — createTempView

**Code**

```python
%sql
select * from my_view where Item_Fat_Content = 'Lf'
```

**Explanation**

Runs SQL in a notebook cell: selects all columns from `my_view` where `Item_Fat_Content` equals `Lf`. String comparison is case-sensitive in some configurations; earlier examples standardize to `LF`, so check the actual values.


## Cell 163 — createTempView

**Code**

```python
df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'Lf'")
```

**Explanation**

Runs the same SQL query through Python using `spark.sql()` and stores the result in `df_sql`.


## Cell 164 — createTempView

**Code**

```python
df_sql.display()
```

**Explanation**

Displays the DataFrame returned by the SQL query.


## Important corrections and notes

1. **Column name typo:** `tem_Identifier` in the schema appears to be a typo for `Item_Identifier`. If the CSV uses `Item_Identifier`, correct it in both schema definitions.

2. **Data types matter:** comparisons and arithmetic such as `Item_Weight > 21` and `Item_Weight * Item_MRP` require numeric columns. Avoid casting numeric columns to strings before calculations.

3. **Null replacement:** the `when()` expression in Cell 34 needs `.otherwise(col('Outlet_Size'))` to preserve non-null values.

4. **Column casing/value consistency:** `LF`, `Lf`, and `Low Fat` are different strings. Standardize values once, then filter using the standardized form.

5. **File paths:** `/Volumes/...` and `/FileStore/...` are environment-specific paths. Replace them with paths available in your Spark environment. Spark output paths are generally directories containing part files.

6. **Parquet path:** Cell 156 writes Parquet to a path ending in `.csv`; rename the path to avoid misleading future readers.

7. **`display()` is environment-specific:** in plain PySpark, use `df.show()` or `df.show(truncate=False)` if `.display()` is unavailable.

8. **Imports and UDFs:** `from pyspark.sql.functions import *` works for practice, but explicit imports improve readability. Built-in Spark functions are usually faster than Python UDFs.


## Topics covered

- Reading CSV data and defining schemas
- Selecting, aliasing, filtering, renaming, and adding columns
- Sorting, limiting, dropping columns, and removing duplicates
- Union and union-by-name
- String and date functions
- Handling null values
- Splitting strings, array indexing, `explode`, and `array_contains`
- Grouping, aggregations, `collect_list`, and pivot tables
- Conditional logic with `when` and `otherwise`
- Inner, left, right, and anti joins
- Window functions: row number, rank, dense rank, and cumulative sums
- User-defined functions (UDFs)
- Writing CSV, Parquet, and catalog tables
- Temporary views and Spark SQL
