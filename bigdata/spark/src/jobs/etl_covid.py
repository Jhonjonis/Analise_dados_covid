from utils.spark_session import create_spark
from utils.helpers import print_step

def run():
    spark = create_spark("ETL-COVID")

    input_path = "/opt/spark/data/raw/dados"
    output_path = "/opt/spark/data/processed/covid"

    print_step("LENDO ARQUIVOS")
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print_step("LIMPANDO DADOS")
    df_clean = df.dropna()

    print_step("TRANSFORMANDO")
    df_grouped = df_clean.groupBy("UF").count()

    print_step("SALVANDO RESULTADO")
    df_grouped.write.mode("overwrite").parquet(output_path)

    print_step("ETL FINALIZADO")

if __name__ == "__main__":
    run()
