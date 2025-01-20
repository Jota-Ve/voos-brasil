import time

import dim_aeroportos
import dim_tempo
from colunas_dataset import ColunasDataset

PATH_DATASET = './BrFlights2/BrFlights2.csv'
ENCODING_DATASET = 'latin-1'

PATH_DIM_COMPANHIAS = 'BrFlights2/DimCompanhias.csv'
PATH_DIM_JUSTIFICATIVAS = 'BrFlights2/DimJustificativa.csv'
PATH_FATO_VOOS = 'BrFlights2/voos.csv'


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


def separar_voos(path: str):
    NULL_CHAR = ''
    aeroportos = dim_aeroportos.map_aeroporto_to_id()
    tempo = dim_tempo.map_tempo_to_id()

    with open(path, encoding='latin1') as entrada, open(PATH_FATO_VOOS, 'w', encoding='latin-1') as saida:
        next(entrada) # pula cabeçalho

        saida.write('id,idCompanhia,idOrigem,idDestino,idPartidaPrevista,idPartidaReal,idJustificativa,atrasoMinutos,cancelado\n')
        linhas_saida = set()
        i = 0
        for i, line in enumerate(entrada):
            if i%200_000 == 0:
                print(f"Lendo linha {i:_}...")

            line_list = line.strip().split(',')
            partida_prev = str(tempo.get(line_list[ColunasDataset.PARTIDA_PREVISTA], NULL_CHAR))
            partida_real = str(tempo.get(line_list[ColunasDataset.PARTIDA_REAL],     NULL_CHAR))

            id_origem = str(aeroportos[float(line_list[ColunasDataset.LAT_ORIG]),
                                       float(line_list[ColunasDataset.LONG_ORIG])])

            id_destino = str(aeroportos[float(line_list[ColunasDataset.LAT_DEST]),
                                        float(line_list[ColunasDataset.LONG_DEST])])

        #     situacao = line_list[ColunasDataset.SITUACAO_VOO]
        #     justificativa = line_list[ColunasDataset.JUSTIFICATIVA]

        #     linhas_saida.add(','.join([partida_prev, id_origem, id_destino,
        #                           partida_real, situacao, justificativa]) + '\n')

        # print(f"Remove {i-len(linhas_saida):_} linhas duplicadas, sobrando {len(linhas_saida):_} linhas.")
        # saida.writelines(linhas_saida)

def separar_tabelas(dataset_path: str):
    t0 = time.perf_counter()
    separar_companhias(dataset_path)
    print(f"Terminou separar_companhias() após {time.perf_counter() - t0:.0f} segundos")
    dim_aeroportos.separar_aeroportos(dataset_path)
    print(f"Terminou separar_aeroportos() após {time.perf_counter() - t0:.0f} segundos")
    separar_justificativas(dataset_path)
    print(f"Terminou separar_justificativas() após {time.perf_counter() - t0:.0f} segundos")
    dim_tempo.separar_tempos(dataset_path)
    print(f"Terminou separar_tempo() após {time.perf_counter() - t0:.0f} segundos")
    # separar_voos(dataset_path)
    # print(f"Terminou separar_voos() após {time.perf_counter() - t0:.0f} segundos")


def main():
    separar_tabelas(PATH_DATASET)



if __name__ == '__main__':
    main()
