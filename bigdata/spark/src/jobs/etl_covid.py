import sys
sys.path.append("/opt/spark/app")

from utils.spark_session import create_spark
from utils.helpers import print_step

from pyspark.sql.functions import col, upper, trim


def run(write_csv=False):
    spark = create_spark("ETL-COVID-PE-MUNICIPIOS")

    input_path = "/opt/spark/data/raw/dados/*.csv"
    output_path = "/opt/spark/data/processed/covid"

    print_step("LENDO CSV SEM INFER SCHEMA (rápido)")
    df = spark.read.csv(
        input_path,
        header=True,
        sep=";"
    )

    print_step("PADRONIZANDO NOMES DE COLUNA")
    df = df.toDF(*[c.strip().replace(" ", "_").replace("-", "_") for c in df.columns])

    print_step("REMOVENDO LINHAS TOTALMENTE VAZIAS (seguro)")
    df = df.dropna(how="all")

    print_step("DETECTANDO COLUNA DE UF")
    possiveis = ["UF", "uf", "Unidade", "Estado", "estado", "sigla_uf"]
    coluna_uf = next((c.replace(" ", "_") for c in possiveis if c.replace(" ", "_") in df.columns), None)

    if not coluna_uf:
        raise Exception(f"Nenhuma coluna UF encontrada. Colunas: {df.columns}")

    print(f" UF detectado: {coluna_uf}")

    print_step("PADRONIZANDO UF (PE para todos os municípios)")
    df = df.withColumn(coluna_uf, upper(col(coluna_uf)))

    print_step("PADRONIZANDO CAMPOS TEXTO")
    for c in df.columns:
        df = df.withColumn(c, trim(col(c)))

    print_step("AMOSTRA")
    df.limit(10).show(truncate=False)

    print_step("SALVANDO PARQUET")
    df.write.mode("overwrite").parquet(output_path)

    if write_csv:
        print_step("SALVANDO CSV")
        df.write.mode("overwrite").option("header", True).csv(output_path + "_csv")

    print_step("ETL FINALIZADO — TODOS MUNICÍPIOS PRESERVADOS")
    return df


if __name__ == "__main__":
    run(write_csv=False)
