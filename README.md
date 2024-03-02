# bigdata_sparksql_map_reduce_data_processing_and_analysis

## Learning Objectives:
The primary learning objectives of this project are as follows:
1. Understand and implement various Spark transformations and actions.
2. Utilize Spark SQL to perform data manipulation and analysis tasks.
3. Employ MapReduce operations for processing external files.
4. Gain proficiency in creating and managing Spark contexts and sessions.

## External File Descriptions:
The project utilizes several external files for data processing and analysis. These files include:
1. **yelp_review.csv**: Contains Yelp review data.
2. **urban_wanderings.txt**: Contains text data to perform analysis.
3. **stop_words.txt**: Contains a list of stop words for text processing.
4. **yelp_review_new.csv**: Contains additional Yelp review data.

## Part 1: Spark Transformations
1. **Map, FlatMap, Filter, Distinct**: Various Spark transformations like map, flatMap, filter, and distinct are applied to RDDs (`numbers`, `texts`, `data`) to demonstrate their functionality.
2. **GroupByKey**: The `groupByKey()` transformation is used to group data by keys in RDD `grades`.
3. **ReduceByKey**: The `reduceByKey()` transformation is employed to reduce data by keys in RDD `grades`.
4. **SortBy**: Sorting operations are performed using `sortBy()` transformation on RDD `grades`.
5. **Set Operations**: Set operations like `subtract()`, `intersection()`, and `union()` are applied to RDDs `data1` and `data2`.

## Part 2: Spark SQL: Data Cleaning & Aggregations
1. **CreateDataFrame**: Data is loaded into Spark DataFrames (`people`, `df`) from Python lists and CSV files using `createDataFrame()`.
2. **Read CSV**: Pandas DataFrame is read from the CSV file `yelp_review.csv`.
3. **CreateOrReplaceTempView**: Temporary views are created from Spark DataFrames (`df`) for running SQL queries.
4. **Schema Definition**: DataFrame schema is inferred and specified explicitly for `df` and `reviews`.
5. **Summary Statistics**: Summary statistics like count, mean, stddev, min, max are calculated for selected columns of DataFrame `reviews`.
6. **Handling Duplicates**: Duplicates are handled using `dropDuplicates()` function.
7. **Imputation of NULL Values**: NULL values are imputed with `-1` using `fillna()` function.
8. **Sorting**: Data is sorted by given columns in ascending and descending orders.
9. **Filter Transformation**: DataFrame is filtered based on specified conditions.
10. **Grouping and Aggregation**: Data is grouped by specific columns and various aggregation functions like avg, stddev, min, max are applied.
11. **Join Operation**: DataFrames are joined using `join()` operation.
12. **Creating New Columns**: New columns are created using `withColumn()` function.
13. **Sorting based on New Columns**: Data is sorted based on newly created columns.

## Part 3: Map Reduce & Data Visualizations
1. **Read and Process External Files**: External files (`urban_wanderings.txt`, `stop_words.txt`) are read and processed using Spark RDDs.
2. **Word Count**: Word count is performed on text data after cleaning and filtering stop words.
3. **Plotting Word Occurrences**: Top word occurrences are plotted using Pandas and Matplotlib.
4. **Replacing Non-Alphanumeric Characters**: Non-alphanumeric characters in words are replaced using Regular Expressions.
5. **Transformations Using Map Reduce**: MapReduce transformations are applied to process data from the CSV file `yelp_review_new.csv`.
