# Importing required libraries
!pip install pyspark

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, length, round, sum, avg, max, min, stddev, count, countDistinct, when
import pandas as pd
import matplotlib.pylab as plt
import re

# Initialize SparkContext and SparkSession
conf = SparkConf().setMaster("local").setAppName("Spark")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("Spark").getOrCreate()

# Loading the datasetss
from urllib.request import urlretrieve
urlretrieve('https://drive.google.com/uc?export=download&id=1JfhqO1qDK963RLnEzNrhjMbiqbYtld-U',
            'yelp_review.csv')
urlretrieve('https://drive.google.com/uc?export=download&id=1_UdUBOzbMqR5Th83iUAxe7mCAwd0zq1U',
            'urban_wanderings.txt')
urlretrieve('https://drive.google.com/uc?export=download&id=1aasyVp2_LnPhop78v6PVoV2nvFfD8hXm',
            'stop_words.txt')
urlretrieve('https://drive.google.com/uc?export=download&id=1dE5B915DkZqEwIgYXBoJMRJPrsmitFKM',
            'yelp_review_new.csv')

# Part 1: Spark Transformations
# Spark Transformation: map, flatMap, filter, distinct
numbers = [1, 2, 3, 4, 5, 6, 7]
numbers = sc.parallelize(numbers)
print(numbers.map(lambda x: x**2).collect())

texts = ["Jaime, Lannister", "Tyrion, Lannister", "Cersei, Lannister",
         "Robb, Stark", "Arya, Stark", "Sansa, Stark"]
texts = sc.parallelize(texts)
print(texts.map(lambda line: line.upper()).collect())
print(texts.map(lambda line: line.lower()).collect())
print(texts.flatMap(lambda line: line.split(', ')).collect())
print(texts.filter(lambda line: "Stark" in line).collect())

data = sc.parallelize([1, 1, 3, -5, 'data', 'Data'])
print(data.distinct().collect())
list_data = sc.parallelize(['1', '1', '3', '-5', 'data', 'Data'])
lower_data = list_data.map(lambda line: line.lower())
print(lower_data.distinct().collect())

# Spark Transformation: groupByKey
grades = [("s1", 100), ("s1", 96), ("s2", 93), ("s3", 75), ("s1", 60)]
grades = sc.parallelize(grades)
grouped_grades = grades.groupByKey().collect()

grouped_grades_list = [(key, list(values)) for key, values in grouped_grades]
for key, values in grouped_grades_list:
    print(key, values)

# Spark Transformation: reduceByKey
grades = [("s1", 100), ("s1", 96), ("s2", 93), ("s3", 75), ("s1", 60)]
grades = sc.parallelize(grades)
print(grades.reduceByKey(lambda v1,v2: v1+v2).collect())
reduced_grades = grades.reduceByKey(lambda v1,v2: v1+v2).collect()
for key, values in reduced_grades:
  print(key, values)

# Spark Transformation: sortBy
## Sort RDD by values in the ascending order
print(grades.sortBy(lambda pair: pair[1]).collect())

## Sort RDD by values in the descending order
print(grades.sortBy(lambda pair: -pair[1]).collect())

## Sort RDD by keys
print(grades.sortBy(lambda pair: pair[0]).collect())
print(grades.sortByKey(ascending=True).collect())
print(grades.sortByKey(ascending=False).collect())

# Spark Transformation: subtract, intersection, union
data1 = sc.parallelize(['Stark', 'Lannister', 'Targaryen'])
data2 = sc.parallelize(['Baratheon', 'Martell', 'Lannister'])
print(data1.subtract(data2).collect())
print(data2.subtract(data1).collect())
print(data1.intersection(data2).collect())
print(data1.union(data2).collect())
print(set(data1.union(data2).collect()))

# Part 2: Spark SQL
# Spark SQL: createDataFrame
data = [('Robb',19),('Sansa',17),('Arya',14),('Bran',12),('Rickon',8)]
rdd = sc.parallelize(data)
people = spark.createDataFrame(rdd, ["Name", "Age"])
people.show()
print(people.take(4))
print(people.head(3))

# Spark SQL: read csv
pdf = pd.read_csv('yelp_review.csv')
print("Displaying top 20 rows of yelp reviews")
print(pdf.head(20))

# Converting pandas dataframe to Spark dataframe
df = spark.createDataFrame(pdf)
print()
print("Displaying top 5 rows of yelp reviews")
df.show(n=5)

# Spark SQL: createOrReplaceTempView
print("Running SQL Query on a Spark DataFrame Temporary View")
df.createOrReplaceTempView("reviews_df")
spark.sql(
"""
    SELECT *
    FROM reviews_df
    WHERE review_count > 500 AND stars > 4
    ORDER BY review_count DESC, stars DESC
"""
).show(n=20)

# Schema definition
df = spark.read.csv('yelp_review.csv', header=True, inferSchema=True)
print('Inferred Schema of the DataFrame:')
df.printSchema()
schema = StructType([
    StructField('name', StringType(), True),
    StructField('postal_code', StringType(), True),
    StructField('stars', FloatType(), True),
    StructField('review_count', IntegerType(), True),
    StructField('is_open', IntegerType(), True)])
reviews = spark.read.csv('yelp_review.csv', header=True, schema=schema)
print('Specified Schema of the DataFrame:')
reviews.printSchema()

# Summary statistics of the DataFrame: count, mean, stddev, min, max
print('Summary statistics of the DataFrame:')
reviews.describe(['stars', 'review_count', 'is_open']).show()

# Handling Duplicates
print('Number of rows before dropping duplicates:', reviews.count())
reviews.dropDuplicates().count()
print('Number of rows after dropping duplicates:', reviews.dropDuplicates().count())

# Imputation of NULL values with -1
print('First 5 rows of the DataFrame before filling the missing values:')
reviews.show(n=5)
print('First 5 rows of the DataFrame after filling the missing values:')
reviews.fillna(value= -1).show(n=5)

# Sorting by given columns in certain orders
print()
print('First 5 rows of sorting the DataFrame by postal_code (dsc) and stars (asc)')
reviews.sort('postal_code', 'stars', ascending=[False, True]).show(n=20)

## Dropping duplicates, and filling missing values, sorting, filtering
reviews = reviews.dropDuplicates().fillna(value=-1)\
    .sort('postal_code', 'stars', ascending=[False, True])
reviews.show(n=5)

# Filter transformation on a dataframe
print('Businesses with a star rating over 4.0:')
reviews.filter(reviews['stars'] > 4.0).show(n=5)
print('Businesses with a star rating below 3.0 AND over 100 reviews:')
reviews.filter((reviews['stars'] < 3.0) & (reviews['review_count'] > 100)).show(n=5)
print('Businesses with a star rating over 4.5 OR located in 85281:')
reviews.filter((reviews['stars'] > 4.5) | (reviews['postal_code'] == '85281')).show(n=5)
print('Businesses not located in 85310, having more than 200 reviews, sorted by review count in ascending and star rating in descending order')
review_filtered = reviews.filter((reviews['postal_code'] != '85310') & (reviews['review_count'] > 200))
review_filtered = review_filtered.sort('review_count', 'stars', ascending=[True,False])
review_filtered.show(20)

# Grouping by specific columns and calculating group statistics
print('The average and standard deviation of star ratings by postal code:')
reviews.groupby('postal_code').agg(
    avg('stars').alias('mean_stars'),
    stddev('stars').alias('std_stars')
).show(n=5)

# Rounding the aggregated fields
print('The average and standard deviation of star ratings (rounded) by postal code:')
reviews.groupby('postal_code').agg(
    round(avg('stars'), 2).alias('mean_stars'),
    round(stddev('stars'), 2).alias('std_stars')
).show(n=5)

# Other aggregate functions: min, max, avg
print('The maximum review count and lowest star rating by postal code:')
reviews.groupby('postal_code').agg(
    max('review_count').alias('max_review_count'),
    min('stars').alias('min_stars')
).show(n=5)

print('The average review count by postal code and open status:')
reviews.groupby(['postal_code', 'is_open']).agg(
    avg('review_count').alias('mean_review_count'),
).show(n=20)

# Aggregate function Examples
print("Average of reviews, and number of businesses of each group based on postal code and open stuts")
reviews_grouped = reviews.groupby(['postal_code', 'is_open']).agg(
    round(avg('review_count'), 1).alias('mean_review_count'),
    count('name').alias('num_businesses')
)
reviews_grouped.show(20)

print("Average of stars and review count of each group based on postal code")
reviews_grouped = reviews.groupby('postal_code').agg(
    round(avg('stars'), 2).alias('mean_stars'),
    round(avg('review_count'), 2).alias('mean_review_count')
)
reviews_grouped.show(n=5)

# Join operation
print("All columns displayed along with the aggregation")
reviews_joined = reviews.join(reviews_grouped, on='postal_code', how='inner')
reviews_joined.show(n=5)

# Creating a new column using withColumn function
reviews_joined = reviews_joined.withColumn(
    'diff_stars',
    col('stars') - col('mean_stars')
)

# Sorting based on the created column
print("Displaying reviews sorted by difference in stars from average, and review count in descending order")
reviews_joined.select(['name', 'postal_code', 'diff_stars', 'review_count'])\
    .sort('diff_stars', 'review_count', ascending=[False, False]).show(n=5)

# Creating another column to showcase comparison
print("")
print("Displaying along with the stars comparison column")
reviews_joined.withColumn(
    'better_than_peers',
    when(col('diff_stars') > 0.5, 'better').
    when(col('diff_stars') < -0.5, 'worse').
    otherwise('similar')
).show(n=10)

# Part 3: Map Reduce
# Read and process external files: MapReduce
stop_words_file = open('stop_words.txt')
stop_words = stop_words_file.read().split()
stop_words_file.close()
urban_wanderings = sc.textFile('urban_wanderings.txt')
words = urban_wanderings.flatMap(lambda line: line.split())
words_cleaned = words.map(lambda word: word.lower()).filter(lambda x: x not in stop_words)
word_counts = words_cleaned.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2)
word_counts_sorted = word_counts.sortBy(lambda x: -x[1])
top_ten_words = word_counts_sorted.take(10)
for word_occurrence in top_ten_words:
    print(word_occurrence[0] + ':\t' + str(word_occurrence[1]))

# Plot top 20 word occurences from the list
top_20_words = word_counts_sorted.take(20)
words, counts = zip(*top_20_words)
print(words)
print(counts)
pd.Series(counts, index=words).plot(kind='bar')
plt.show()

# Replacing non-alphanumeric non-space characters in the words with empty string using RegEx
wordcounts = (words_cleaned.flatMap(lambda word: word.split())
                       .map(lambda word: re.sub(r'[^\w\s]','',word).lower())
                       .filter(lambda word: len(word) > 0)
                       .filter(lambda word: word not in stop_words)
                       .map(lambda word: (word, 1))
                       .reduceByKey(lambda x,y: x+y)
                       .takeOrdered(20, lambda w: -w[1]))
wordcounts

# Plotting the word count occurences after cleaning the data
words, counts = zip(*wordcounts)
print(words)
print(counts)
pd.Series(counts, index=words).plot(kind='bar')
plt.show()

# Defining lineparser function
def line_parser(line):
    fields = line.split(',')
    postal_code = fields[1]
    stars = fields[2]
    return (postal_code, stars)

# Transformations using Map Reduce
lines = sc.textFile("yelp_review_new.csv")
header = lines.first()
lines = lines.filter(lambda line: line != header)
rdd = lines.map(line_parser)
print('First 5 pairs: ')
for pair in rdd.take(5):
    print(pair[0] + ':', pair[1])
print()
print("Performing the following transformations:")
print("Filtering out missing values, Converting star ratings to float, Creating postal code: (stars, 1) pairs, Getting total str ratings by postal code")
rdd = lines.map(line_parser)
rdd = rdd.filter(lambda line: (len(line[0]) != 0) and (len(line[1]) != 0))
rdd = rdd.map(lambda line: (line[0], float(line[1])))
rdd = rdd.map(lambda line: (line[0], (line[1], 1)))
totals_by_code = rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# Can be performaed in a single line
# totals_by_code = lines.map(line_parser).filter(lambda line: (len(line[0]) != 0) and (len(line[1]) != 0)).map(lambda line: (line[0], float(line[1]))).map(lambda line: (line[0], (line[1], 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

print()
print('First 5 reduced pairs: ')
for pair in totals_by_code.take(5):
    print(pair[0] + ':', pair[1])
averages_by_code = totals_by_code.map(lambda x: (x[0], x[1][0] / x[1][1]))
averages_by_code_sorted = averages_by_code.sortBy(lambda x: (-x[1], x[0]))
print()
print("Displaying average star ratings by postal code, and sorting the results")
for result in averages_by_code_sorted.take(5):
    print(result[0] + ':', result[1])

# Stop SparkContext
sc.stop()