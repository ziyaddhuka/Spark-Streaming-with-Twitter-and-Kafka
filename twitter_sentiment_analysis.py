
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, concat_ws , lit
from  pyspark.ml.feature import StopWordsRemover, RegexTokenizer


def sentiment_to_value(x):
    if x == 'positive':
        return 1
    elif x=='negative':
        return -1
    else:
        return 0


if __name__ == "__main__":
    # if len(sys.argv) != 4:
    #     print("""
    #     Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
    #     """, file=sys.stderr)
    #     sys.exit(-1)
    #
    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]
    # spark = sparknlp.start()
    # spark = SparkSession\
    #     .builder\
    #     .appName("Twitter_sentiment")\
    #     .config("spark.jars", "/home/zad/anaconda3/lib/python3.7/site-packages/sparknlp/spark-nlp-assembly-3.1.3.jar")\
    #     .config("spark.kryoserializer.buffer.max", "1000m")\
    #     .getOrCreate()
    # spark = sparknlp.start()

    spark = SparkSession.builder \
    .appName("Spark NLP")\
    .master("local[*]")\
    .config("spark.driver.memory","15G")\
    .config("spark.driver.maxResultSize", "2G") \
    .config("spark.jars", "/home/zad/anaconda3/lib/python3.7/site-packages/sparknlp/spark-nlp-assembly-3.1.3.jar")\
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .getOrCreate()
    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .option("failOnDataLoss", "false")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    lines = lines.toDF('text')

    MODEL_NAME='sentimentdl_use_twitter'

    documentAssembler = DocumentAssembler().setInputCol("text").setOutputCol("document")

    use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en").setInputCols(["document"]).setOutputCol("sentence_embeddings")

    sentimentdl = SentimentDLModel.pretrained(name=MODEL_NAME, lang="en").setInputCols(["sentence_embeddings"]).setOutputCol("sentiment")

    nlpPipeline = Pipeline(
          stages = [
              documentAssembler,
              use,
              sentimentdl
          ])

    empty_df = spark.createDataFrame([['']]).toDF("text")
    pipelineModel = nlpPipeline.fit(empty_df)
    result = pipelineModel.transform(lines)
    res = result.select(F.explode(F.arrays_zip('document.result', 'sentiment.result')).alias("cols")).select(F.expr("cols['0']").alias("key"),F.expr("cols['1']").alias("sentiment"))


    func_sentiment = udf(sentiment_to_value)
    res = res.withColumn('sentiment_value', func_sentiment('sentiment'))
    res = res.select("key",to_json(struct("sentiment","sentiment_value")).alias("value"))

    tokenizer = RegexTokenizer(pattern=r'(?:\p{Punct}|\s)+', inputCol='key', outputCol='text_temp')
    txt_arr = tokenizer.transform(res)
    remover = StopWordsRemover(inputCol='text_temp', outputCol='filtered_tweet')
    vector_no_stopw_df = remover.transform(txt_arr)
    key_column = vector_no_stopw_df.select('key', 'value', concat_ws(' ', vector_no_stopw_df.filtered_tweet).alias('filtered_tweet'))
    res = key_column.drop('key').select(col('filtered_tweet').alias('key'),'value')

    query = res\
      .writeStream\
      .format("kafka")\
      .option("kafka.bootstrap.servers", "localhost:9092")\
      .option("path", "./parc")\
      .option("checkpointLocation", "./zd")\
      .option("topic", "olympics_sentiment")\
      .start()

    query.awaitTermination()
