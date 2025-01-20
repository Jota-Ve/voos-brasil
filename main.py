from datetime import datetime
import time

import dim_aeroportos
import dim_companhias
import dim_justificativas
import dim_tempo
from colunas_dataset import ColunasDataset

PATH_DATASET = './BrFlights2/BrFlights2.csv'
ENCODING_DATASET = 'latin-1'

PATH_FATO_VOOS = 'BrFlights2/voos.csv'


def separar_voos(path: str):
    NULL_CHAR = ''
    aeroportos = dim_aeroportos.map_aeroporto_to_id()
    tempo = dim_tempo.map_tempo_to_id()
    companhia = dim_companhias.map_companhia_to_id()
    justificativa = dim_justificativas.map_justificativa_to_id()

    with open(path, encoding='latin1') as entrada, open(PATH_FATO_VOOS, 'w', encoding='latin-1') as saida:
        next(entrada) # pula cabeçalho

        saida.write('id,idCompanhia,idOrigem,idDestino,idPartidaPrevista,idPartidaReal,idJustificativa,atrasoMinutos,cancelado\n')
        linhas_saida = set()
        i = 0
        for i, line in enumerate(entrada):
            if i%200_000 == 0:
                print(f"Lendo linha {i:_}...")

            line_list = line.strip().split(',')
            id_companhia = companhia[line_list[ColunasDataset.COMPANHIA_AEREA]]

            id_origem = aeroportos[float(line_list[ColunasDataset.LAT_ORIG]),
                                   float(line_list[ColunasDataset.LONG_ORIG])]

            id_destino = aeroportos[float(line_list[ColunasDataset.LAT_DEST]),
                                    float(line_list[ColunasDataset.LONG_DEST])]

            partida_prevista = line_list[ColunasDataset.PARTIDA_PREVISTA]
            partida_real     = line_list[ColunasDataset.PARTIDA_REAL]

            id_partida_prev = tempo.get(partida_prevista, NULL_CHAR)
            id_partida_real = tempo.get(partida_real,     NULL_CHAR)

            atraso_minutos = round((
                datetime.fromisoformat(partida_real)
                - datetime.fromisoformat(partida_prevista)
            ).total_seconds() / 60)

            id_justificativa = justificativa.get(line_list[ColunasDataset.JUSTIFICATIVA], NULL_CHAR)

        #     linhas_saida.add(','.join(map(str, [id_companhia, id_origem, id_destino, id_partida_prev,
        #                                         id_partida_real, id_justificativa, atraso_minutos])) + '\n')

        # print(f"Removeu {i-len(linhas_saida):_} linhas duplicadas, sobrando {len(linhas_saida):_} linhas.")
        # saida.writelines(linhas_saida)


def separar_tabelas(dataset_path: str):
    t0 = time.perf_counter()
    # dim_companhias.separar_companhias(dataset_path)
    # print(f"Terminou separar_companhias() após {time.perf_counter() - t0:.0f} segundos")
    # dim_aeroportos.separar_aeroportos(dataset_path)
    # print(f"Terminou separar_aeroportos() após {time.perf_counter() - t0:.0f} segundos")
    # separar_justificativas(dataset_path)
    # print(f"Terminou separar_justificativas() após {time.perf_counter() - t0:.0f} segundos")
    # dim_tempo.separar_tempos(dataset_path)
    # print(f"Terminou separar_tempo() após {time.perf_counter() - t0:.0f} segundos")
    separar_voos(dataset_path)
    print(f"Terminou separar_voos() após {time.perf_counter() - t0:.0f} segundos")


def main():
    separar_tabelas(PATH_DATASET)



if __name__ == '__main__':
    main()
