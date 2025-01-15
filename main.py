import numpy as np
import os
import time
from pyspark.sql import SparkSession


def spark_shit():
    PYTHON_PATH = r'.venv\Scripts\python.exe'
    # os.environ['PYSPARK_DRIVER_PYTHON'] = PYTHON_PATH
    # os.environ['%HADOOP_HOME%'] = r'C:\Users\JV\Downloads\spark-3.5.4-bin-hadoop3\spark-3.5.4-bin-hadoop3'

    # spark = SparkSession.builder.appName("demo").getOrCreate()
    spark = SparkSession.builder \
        .appName("demo") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.pyspark.python", PYTHON_PATH) \
        .config("spark.pyspark.driver.python", PYTHON_PATH) \
        .getOrCreate()

    print(dir(spark))

    df = spark.createDataFrame([(1, 'a'), (2, 'b')])
    print(df.head())


def read_numpy(path: str, encoding: str, max_rows: int|None=None):
    return np.loadtxt(path, encoding=encoding, delimiter=',', dtype=str, max_rows=max_rows)


def read_csv(path: str, encoding: str, max_rows: int|None = None):
    with open(path, encoding=encoding) as f:
        if max_rows:
            return [line.split(',') for line, _ in zip(f, range(max_rows))]
        
        return [line.split(',') for line in f]


def separar_aeronaves(path: str):
    linhas_to_write = set()
    with open(path, encoding='latin1') as entrada, open('BrFlights2/aeronaves.csv', 'w', encoding='latin-1') as saida:
        cabecalho = next(saida)
        for line in entrada:
            aeronave, companhia = line.split(',', maxsplit=2)[:2]
            linhas_to_write.add(f'{aeronave},{companhia}\n')
        
        saida.write(cabecalho)
        for line in sorted(linhas_to_write):
            saida.write(line)
            

def separar_aeroportos(path: str):
    linhas_saida: set[tuple[str, ...]] = set()
    cabecalho = ''
    with open(path, encoding='latin1') as entrada, open('BrFlights2/aeroportos.csv', 'w', encoding='latin-1') as saida:
        next(entrada) # pula cabeçalho
        for line in entrada:
            aero_orig, cidade_orig, estado_orig, pais_orig, aero_dest, cidade_dest, estado_dest, pais_dest, long_dest, lat_dest, long_orig, lat_orig = line.rsplit(',', maxsplit=12)[1:] # Vem con '\n' no final
            lat_orig = lat_orig.strip() #Remove \n
            linhas_saida.add((pais_orig, estado_orig, cidade_orig, aero_orig, lat_orig, long_orig))
            linhas_saida.add((pais_dest, estado_dest, cidade_dest, aero_dest, lat_dest, long_dest))
       
        saida.write('pais,estado,cidade,aeroporto,latitude,longitue\n')
        for line in sorted(linhas_saida):
            saida.write(','.join(line) + '\n')
            


def main():
    DATASET_PATH = './BrFlights2/BrFlights2.csv'
    DATASET_ENCODING = 'latin-1'
    MAX_ROWS = None
    
    separar_aeronaves(DATASET_PATH)
    separar_aeroportos(DATASET_PATH)
    
    
    
    
if __name__ == '__main__':
    main()
