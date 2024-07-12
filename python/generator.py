# Dummy Data : jeu de donnée généré pour du test, de l'analyse ou des démonstrations.
# https://medium.com/@rianying/generating-dummy-data-with-python-a-practical-guide-2ec7bea5a80b
# Mock data generator : https://www.tinybird.co/blog-posts/mockingbird-announcement-mock-data-generator

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as sf
from faker import Faker
import random
from typing import List
import time


def generator(fake, sample_size:int=1) -> List:
    """Génére un échantillon de données aléatoires.

    Keywords arguments:
    fake -- objet Faker()
    sample_size -- taille de l'échantillon (par défaut 1)
    """
    sample = []
    # Ajoute 1 à n lignes de données
    for i in range(sample_size):
        sample.append({"name":fake.name(), "address":fake.address(), "city":fake.city, "phone": fake.phone_number()})
    return sample


def spark_df(sample, spark, schema):
    """Opération(s) Spark appliqué à un Dataframe Spark.
    
    Keywords arguments:
    sample -- échantillon de données
    spark -- objet de session Spark
    schema -- schéma de type de données du DataFrame Spark (non utilisé)
    """
    df_spark = spark.createDataFrame(sample)#, schema)
    df_with_length = df_spark.withColumn("name_length", sf.length(df_spark["name"]))
    df_filtered = df_with_length.filter(sf.length(df_with_length["name"]) < 12)
    df_filtered.show() 
    # df_filtered.select("name", "name_length").show()


if __name__ == "__main__":
    # Création d'un objet de session Spark
    spark = SparkSession.builder.appName("PySpark Example").getOrCreate()
    # Création d'un objet Faker()
    fake = Faker()
    # Schéma de type de données du DataFrame Spark # Non utilisé
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("phone", StringType(), True)
    ])
    # Génére et envoie indéfiniment un sample de données aux instance Spark
    while True:
        sample = generator(fake, random.randint(1,5))
        spark_df(sample, spark, schema)
        time.sleep(1) # Pourrait être randomisé et testé l'envoi par Batch de données
