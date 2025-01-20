import numpy as np
from pyspark.sql import SparkSession

from colunas_dataset import ColunasDataset


def spark_things():
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

    df = spark.createDataFrame([(1, 'a'),
    (2, 'b')])
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
        cabecalho = next(entrada).split(',', maxsplit=2)[:2]
        for line in entrada:
            aeronave, companhia = line.split(',', maxsplit=2)[:2]
            linhas_to_write.add(f'{aeronave},{companhia}')
        
        saida.write(f"id,{','.join(cabecalho)}\n")
        for _id, line in enumerate(sorted(linhas_to_write), start=1):
            saida.write(f"{_id},{line}\n")
            

def separar_aeroportos(path: str):
    linhas_saida: set[tuple[str, ...]] = set()
    with open(path, encoding='latin1') as entrada, open('BrFlights2/aeroportos.csv', 'w', encoding='latin-1') as saida:
        next(entrada) # pula cabeçalho
        for line in entrada:
            aero_orig, cidade_orig, estado_orig, pais_orig, aero_dest, cidade_dest, estado_dest, pais_dest, long_dest, lat_dest, long_orig, lat_orig = line.rsplit(',', maxsplit=12)[1:] # Vem con '\n' no final
            lat_orig = lat_orig.strip() #Remove \n
            linhas_saida.add((pais_orig, estado_orig, cidade_orig, aero_orig, lat_orig, long_orig))
            linhas_saida.add((pais_dest, estado_dest, cidade_dest, aero_dest, lat_dest, long_dest))
       
        saida.write('id,pais,estado,cidade,aeroporto,latitude,longitue\n')
        for _id, line in enumerate(sorted(linhas_saida), start=1):
            saida.write(f"{_id},{','.join(line)}\n")

    
    
def separar_voos(path: str):
    aeronaves = map_aeronave_to_id()
    aeroportos = map_aeroporto_to_id()
    
    with open(path, encoding='latin1') as entrada, open('BrFlights2/voos.csv', 'w', encoding='latin-1') as saida:
        next(entrada) # pula cabeçalho
        
        saida.write('idAeronave,partidaPrevista,idOrigem,idDestino,tipoLinha,'
                    'partidaReal,chegadaPrevista,chegadaReal,situacao,justificativa\n')
        linhas_saida = set()
        i = 0
        for i, line in enumerate(entrada):
            if i%200_000 == 0:
                print(f"Lendo linha {i:_}...")
                
            line_list = line.strip().split(',')
            id_aeronave = str(aeronaves[line_list[ColunasDataset.VOOS]])
            partida_prev = line_list[ColunasDataset.PARTIDA_PREVISTA]
            if (partida_prev := line_list[ColunasDataset.PARTIDA_REAL].strip('Z')) == 'NA': 
                partida_prev = ''
            
            id_origem = str(aeroportos[float(line_list[ColunasDataset.LAT_ORIG]),
                                   float(line_list[ColunasDataset.LONG_ORIG])])
            
            id_destino = str(aeroportos[float(line_list[ColunasDataset.LAT_DEST]),
                                    float(line_list[ColunasDataset.LONG_DEST])])
            
            tipo_linha = line_list[ColunasDataset.CODIGO_TIPO_LINHA]
            
            if (partida_real := line_list[ColunasDataset.PARTIDA_REAL].strip('Z')) == 'NA':
                partida_real = ''
            if (chegada_prev := line_list[ColunasDataset.CHEGADA_PREVISTA].strip('Z')) == 'NA':
                chegada_prev = ''
            if (chegada_real := line_list[ColunasDataset.CHEGADA_REAL].strip('Z')) == 'NA':
                chegada_real = ''
                
            situacao = line_list[ColunasDataset.SITUACAO_VOO]
            justificativa = line_list[ColunasDataset.CODIGO_JUSTIFICATIVA]
            
            linhas_saida.add(','.join([id_aeronave, partida_prev, id_origem, id_destino, tipo_linha, 
                                  partida_real, chegada_prev, chegada_real, situacao, justificativa]) + '\n')
        
        print(f"Remove {i-len(linhas_saida):_} linhas duplicadas, sobrando {len(linhas_saida):_} linhas.")
        saida.writelines(linhas_saida)
        
            
def map_aeronave_to_id(path='./BrFlights2/aeronaves.csv') -> dict[str, int]:
    with open(path, encoding='latin-1') as f:
        next(f)
        return {aeronave: int(_id) for _id, aeronave, copanhia in map(lambda l: l.strip().split(','), f)}


def map_aeroporto_to_id(path='./BrFlights2/aeroportos.csv') -> dict[tuple[float, float], int]:
    with open(path, encoding='latin-1') as f:
        next(f)
        return {(float(latitude), float(longitude)): int(_id) for _id, *_ , latitude, longitude in map(lambda l: l.strip().split(','), f)}
                

def separar_tabelas(dataset_path: str):
    separar_aeronaves(dataset_path)
    separar_aeroportos(dataset_path)
    separar_voos(dataset_path)



def main():
    DATASET_PATH = './BrFlights2/BrFlights2.csv'
    DATASET_ENCODING = 'latin-1'

    separar_tabelas(DATASET_PATH)
    
    
    
    
    
if __name__ == '__main__':
    main()
