from utils.spark_session import create_spark
from utils.helpers import print_step

def run():
    spark = create_spark("WordCount")

    input_file = "/opt/spark/data/raw/dados/exemplo.txt"
    output_dir = "/opt/spark/output/wordcount"

    print_step("LENDO ARQUIVO TEXTO")
    rdd = spark.sparkContext.textFile(input_file)

    counts = (
        rdd.flatMap(lambda line: line.split())
           .map(lambda word: (word, 1))
           .reduceByKey(lambda a, b: a + b)
    )

    print_step("SALVANDO OUTPUT")
    counts.saveAsTextFile(output_dir)

    print_step("FINALIZADO")

if __name__ == "__main__":
    run()
