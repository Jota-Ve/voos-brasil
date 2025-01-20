import time

import dim_aeroportos
import dim_companhias
import dim_justificativas
import dim_tempo
import fato_voos

PATH_DATASET = './BrFlights2/BrFlights2.csv'
ENCODING_DATASET = 'latin-1'


def separar_tabelas(dataset_path: str):
    t0 = time.perf_counter()
    dim_companhias.separar_companhias(dataset_path)
    print(f"Terminou separar_companhias() após {time.perf_counter() - t0:.0f} segundos")
    dim_aeroportos.separar_aeroportos(dataset_path)
    print(f"Terminou separar_aeroportos() após {time.perf_counter() - t0:.0f} segundos")
    dim_justificativas.separar_justificativas(dataset_path)
    print(f"Terminou separar_justificativas() após {time.perf_counter() - t0:.0f} segundos")
    dim_tempo.separar_tempos(dataset_path)
    print(f"Terminou separar_tempo() após {time.perf_counter() - t0:.0f} segundos")
    fato_voos.separar_voos(dataset_path)
    print(f"Terminou separar_voos() após {time.perf_counter() - t0:.0f} segundos")


def main():
    separar_tabelas(PATH_DATASET)



if __name__ == '__main__':
    main()
