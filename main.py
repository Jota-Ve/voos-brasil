from datetime import datetime
import time
import numpy as np
from pyspark.sql import SparkSession

from colunas_dataset import ColunasDataset

PATH_DATASET = './BrFlights2/BrFlights2.csv'
ENCODING_DATASET = 'latin-1'

PATH_DIM_COMPANHIAS = 'BrFlights2/DimCompanhias.csv'
PATH_DIM_AEROPORTOS = 'BrFlights2/DimAeroportos.csv'
PATH_DIM_JUSTIFICATIVAS = 'BrFlights2/DimJustificativa.csv'
PATH_DIM_TEMPO = 'BrFlights2/DimTempo.csv'
PATH_FATO_VOOS = 'BrFlights2/voos.csv'


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


def separar_companhias(path: str):
    linhas_to_write = set()
    with open(path, encoding='latin1') as entrada, open(PATH_DIM_COMPANHIAS, 'w', encoding='latin-1') as saida:
        cabecalho = (next(entrada).split(',', maxsplit=2)[ColunasDataset.COMPANHIA_AEREA],)
        for line in entrada:
            companhia = line.split(',', maxsplit=2)[ColunasDataset.COMPANHIA_AEREA]
            linhas_to_write.add(companhia)

        saida.write(f"id,{','.join(cabecalho)}\n")
        for _id, line in enumerate(sorted(linhas_to_write), start=1):
            saida.write(f"{_id},{line}\n")


def separar_aeroportos(path: str):
    linhas_saida: set[tuple[str, ...]] = set()
    with open(path, encoding='latin1') as entrada, open(PATH_DIM_AEROPORTOS, 'w', encoding='latin-1') as saida:
        next(entrada) # pula cabeçalho
        for line in entrada:
            aero_orig, cidade_orig, estado_orig, pais_orig, aero_dest, cidade_dest, estado_dest, pais_dest, long_dest, lat_dest, long_orig, lat_orig = line.rsplit(',', maxsplit=12)[1:] # Vem con '\n' no final
            lat_orig = lat_orig.strip() #Remove \n
            linhas_saida.add((pais_orig, estado_orig, cidade_orig, aero_orig, f'{float(lat_orig):.7f}', f'{float(long_orig):.7f}'))
            linhas_saida.add((pais_dest, estado_dest, cidade_dest, aero_dest, f'{float(lat_dest):.7f}', f'{float(long_dest):.7f}'))

        saida.write('id,pais,estado,cidade,nome,latitude,longitude\n')
        for _id, line in enumerate(sorted(linhas_saida), start=1):
            saida.write(f"{_id},{','.join(line)}\n")


def separar_justificativas(path: str):
    linhas_to_write = set()
    with open(path, encoding='latin1') as entrada, open(PATH_DIM_JUSTIFICATIVAS, 'w', encoding='latin-1') as saida:
        cabecalho = (next(entrada).split(',', maxsplit=ColunasDataset.JUSTIFICATIVA +1)[ColunasDataset.JUSTIFICATIVA],)
        for line in entrada:
            justificativa = line.split(',', maxsplit=ColunasDataset.JUSTIFICATIVA +1)[ColunasDataset.JUSTIFICATIVA]
            linhas_to_write.add(justificativa)

        saida.write(f"id,{','.join(cabecalho)}\n")
        for _id, line in enumerate(sorted(linhas_to_write), start=1):
            saida.write(f"{_id},{line}\n")


def separar_tempo(path: str):
    #TODO: Talvez remover os zeros a esquerda de mes, dia, hora e minuto

    linhas_to_write: set[tuple[str, ...]] = set()

    def _separar_data_iso(data: str): return data[:4], data[5:7], data[8:10]

    def _separar_hora_iso(tempo: str): return tempo[:2], tempo[3:5]

    with open(path, encoding='latin1') as entrada, open(PATH_DIM_TEMPO, 'w', encoding='latin-1') as saida:
        next(entrada) # ignora cabecalho
        for line in entrada:
            line_list = line.strip().split(',')
            partida_data, partida_hora = line_list[ColunasDataset.PARTIDA_PREVISTA].strip('Z').split('T')
            linhas_to_write.add(_separar_data_iso(partida_data) + _separar_hora_iso(partida_hora))

            if not line_list[ColunasDataset.PARTIDA_REAL] == 'NA':
                partida_data, partida_hora = line_list[ColunasDataset.PARTIDA_REAL].strip('Z').split('T')
                linhas_to_write.add(_separar_data_iso(partida_data) + _separar_hora_iso(partida_hora))

        saida.write(f"id,ano,mes,dia,hora,minuto\n")
        for _id, line in enumerate(sorted(linhas_to_write), start=1):
            saida.write(f"{_id},{','.join(line)}\n")


def separar_voos(path: str):
    aeronaves = map_aeronave_to_id()
    aeroportos = map_aeroporto_to_id()

    with open(path, encoding='latin1') as entrada, open(PATH_FATO_VOOS, 'w', encoding='latin-1') as saida:
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
            justificativa = line_list[ColunasDataset.JUSTIFICATIVA]

            linhas_saida.add(','.join([id_aeronave, partida_prev, id_origem, id_destino, tipo_linha,
                                  partida_real, chegada_prev, chegada_real, situacao, justificativa]) + '\n')

        print(f"Remove {i-len(linhas_saida):_} linhas duplicadas, sobrando {len(linhas_saida):_} linhas.")
        saida.writelines(linhas_saida)


def map_aeronave_to_id(path='./BrFlights2/aeronaves.csv') -> dict[str, int]:
    with open(path, encoding='latin-1') as f:
        next(f)
        return {aeronave: int(_id) for _id, aeronave, copanhia in map(lambda l: l.strip().split(','), f)}


def map_aeroporto_to_id(path=PATH_DIM_AEROPORTOS) -> dict[tuple[float, float], int]:
    with open(path, encoding='latin-1') as f:
        next(f)
        return {(float(latitude), float(longitude)): int(_id) for _id, *_ , latitude, longitude in map(lambda l: l.strip().split(','), f)}


def separar_tabelas(dataset_path: str):
    t0 = time.perf_counter()
    separar_companhias(dataset_path)
    print(f"Terminou separar_companhias() após {time.perf_counter() - t0:.0f} segundos")
    separar_aeroportos(dataset_path)
    print(f"Terminou separar_aeroportos() após {time.perf_counter() - t0:.0f} segundos")
    separar_justificativas(dataset_path)
    print(f"Terminou separar_justificativas() após {time.perf_counter() - t0:.0f} segundos")
    separar_tempo(dataset_path)
    print(f"Terminou separar_tempo() após {time.perf_counter() - t0:.0f} segundos")
    # separar_voos(dataset_path)
    # print(f"Terminou separar_voos() após {time.perf_counter() - t0:.0f} segundos")


def main():
    separar_tabelas(PATH_DATASET)



if __name__ == '__main__':
    main()
