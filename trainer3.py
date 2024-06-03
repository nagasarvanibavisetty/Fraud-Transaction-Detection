from pyspark.sql.functions import lit, current_date, datediff, year, month, dayofmonth, udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import when
from pyspark.sql.functions import year, month, dayofmonth, hour, col, when, dayofweek
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as sum_  # Avoid conflict with Python's sum
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LinearSVC

# Create Spark session
spark = SparkSession.builder.appName("Data Processing").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# Load data (assuming CSV for example)
df = spark.read.csv("fraudTest.csv", header=True, inferSchema=True)

# Filter rows where 'is_fraud' is 1
fraudulent_transactions = df.filter(df['is_fraud'] == 1)


# Type conversion to string
df = df.withColumn("street", df["street"].cast("string"))
df = df.withColumn("city", df["city"].cast("string"))
df = df.withColumn("state", df["state"].cast("string"))

# Aggregation by 'state'
state_counts = df.groupBy("state").agg(
    count("cc_num").alias("Cardholders"),
    count("merchant").alias("Merchants"),
    sum_("is_fraud").alias("Fraudulent Transactions")
)


# Type Casting to String
df = df.withColumn("first", col("first").cast("string"))
df = df.withColumn("last", col("last").cast("string"))
df = df.withColumn("gender", col("gender").cast("string"))
df = df.withColumn("street", col("street").cast("string"))
df = df.withColumn("city", col("city").cast("string"))
df = df.withColumn("state", col("state").cast("string"))

# Extracting Date Components
df = df.withColumn("trans_year", year("trans_date_trans_time"))
df = df.withColumn("trans_month", month("trans_date_trans_time"))
df = df.withColumn("trans_day", dayofmonth("trans_date_trans_time"))
df = df.withColumn("dob_year", year("dob"))
df = df.withColumn("dob_month", month("dob"))
df = df.withColumn("dob_day", dayofmonth("dob"))
df = df.withColumn("trans_hour", hour("trans_date_trans_time"))
df = df.withColumn("trans_day_of_week", dayofweek("trans_date_trans_time"))

# Custom Transformation for 'uncommon_merchant'
# This requires a more complex operation because it involves a conditional count, which is not straightforward in PySpark.
# We need to compute the frequency of each merchant, then apply a condition to create a new column.
merchant_counts = df.groupBy("merchant").count()
df = df.join(merchant_counts, on="merchant", how="left")
df = df.withColumn("uncommon_merchant", when(col("count") < 10, 1).otherwise(0))
df = df.drop("count")  # Clean up the extra 'count' column used for calculation

# Encode 'gender' assuming 'M' as 1, adjust as necessary for actual data
df = df.withColumn("gender_encoded", when(col("gender") == "M", lit(1)).otherwise(lit(0)))

# Calculate 'age' assuming 'dob' is a date column
df = df.withColumn("age", (year(current_date()) - year("dob")))

# For 'amt_zscore', assuming a grouped z-score needs a window function or grouping by certain criteria
from pyspark.sql.window import Window
from pyspark.sql.functions import stddev, mean
windowSpec = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df = df.withColumn("amt_mean", mean("amt").over(windowSpec))
df = df.withColumn("amt_stddev", stddev("amt").over(windowSpec))
df = df.withColumn("amt_zscore", (col("amt") - col("amt_mean")) / col("amt_stddev"))

# Calculate 'time_duration' if it's between two events, need more info or assumptions for calculation
df = df.withColumn("time_duration", lit(5))  # Placeholder value

# Ensure the column for 'transaction_day_of_week' matches requirement, if needed adjust from 'trans_date_trans_time'
df = df.withColumn("transaction_day_of_week", dayofweek("trans_date_trans_time"))

# Renaming the unnamed column to 'Id'
df = df.withColumnRenamed("Unnamed: 0", "Id")

# Define window for calculating global stats
windowSpec = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# Calculate global mean and standard deviation for the amount
df = df.withColumn("global_amt_mean", mean("amt").over(windowSpec))
df = df.withColumn("global_amt_std", stddev("amt").over(windowSpec))

# Feature: Unusual Amount
df = df.withColumn("unusual_amount", when(df["amt"] > (df["global_amt_mean"] + 3 * df["global_amt_std"]), 1).otherwise(0))

# Feature: Uncommon Merchant
# First calculate the merchant counts
merchant_counts = df.groupBy("merchant").agg(count("*").alias("merchant_count"))
df = df.join(merchant_counts, on="merchant", how="left")
df = df.withColumn("uncommon_merchant", when(col("merchant_count") < 10, 1).otherwise(0))

# Feature: Late Night Transaction
df = df.withColumn("late_night_transaction", when((col("trans_hour") < 6) | (col("trans_hour") > 20), 1).otherwise(0))

# Cleanup extra columns
df = df.drop("global_amt_mean", "global_amt_std", "merchant_count")


df.printSchema()

# Define indexers for each categorical column
indexers = [StringIndexer(inputCol=column, outputCol=column + "_Index", stringOrderType="frequencyDesc") for column in ["city", "state", "gender", "merchant","category","first","last","street","job","trans_num"]]

# Pipeline to process indexers
pipeline = Pipeline(stages=indexers)

# Fit and transform the DataFrame
df = pipeline.fit(df).transform(df)

# Drop original columns and rename index columns to original names
for column in ["city", "state", "gender", "merchant","category","first","last","street","job","trans_num"]:
    df = df.drop(column)
    df = df.withColumnRenamed(column + "_Index", column)

# Define features and target variable
features = ['amt', 'unusual_amount', 'uncommon_merchant', 'late_night_transaction']
target = 'is_fraud'

# Assemble features into feature vector
assembler = VectorAssembler(inputCols=features, outputCol="features")

# Split data into train and test sets
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)



# Train Random Forest classifier-----------------------------------------------------------
rf = RandomForestClassifier(featuresCol="features", labelCol=target, numTrees=100, seed=42)

# Set up the Pipeline
pipeline = Pipeline(stages=[assembler, rf])

# Fit the model
model = pipeline.fit(train_data)

# Predict on test set
predictions = model.transform(test_data)

# Evaluate model performance
evaluator = MulticlassClassificationEvaluator(labelCol=target, predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

# Confusion Matrix
conf_matrix = predictions.groupBy(target).pivot("prediction").count().na.fill(0)

# Print accuracy and confusion matrix
print("Accuracy in Random Forest:", accuracy)
conf_matrix.show()


# Setup the feature columns into a feature vector
assembler = VectorAssembler(inputCols=features, outputCol="features")

# Initialize the GBT classifier----------------------------------------------------------------
gbt = GBTClassifier(featuresCol="features", labelCol=target, maxIter=100, seed=42)

# Setup the Pipeline
pipeline = Pipeline(stages=[assembler, gbt])

# Fit the model on the training data
model = pipeline.fit(train_data)

# Predict on the test data
predictions = model.transform(test_data)

# Evaluate model performance using accuracy as the metric
evaluator = MulticlassClassificationEvaluator(
    labelCol=target, 
    predictionCol="prediction", 
    metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)

# Generate the confusion matrix
conf_matrix = predictions.groupBy(target).pivot("prediction").count().na.fill(0)

# Output results
print("Accuracy in GBT:", accuracy)
conf_matrix.show()


# Initialize the SVM classifier-------------------------------------------------------------
svm = LinearSVC(featuresCol="features", labelCol=target)

# Add to the pipeline
pipeline = Pipeline(stages=[assembler, svm])

# Fit the model
model = pipeline.fit(train_data)

# Predict on test set
predictions = model.transform(test_data)

# Evaluate model performance
evaluator = MulticlassClassificationEvaluator(labelCol=target, predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

# Confusion Matrix
conf_matrix = predictions.groupBy(target).pivot("prediction").count().na.fill(0)

# Print accuracy and confusion matrix
print("Accuracy in SVM:", accuracy)
conf_matrix.show()



