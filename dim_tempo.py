from colunas_dataset import ColunasDataset

PATH_DIM_TEMPO = 'BrFlights2/DimTempo.csv'

def separar_tempos(path: str):
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


def map_tempo_to_id(path=PATH_DIM_TEMPO) -> dict[str, int]:
    with open(path, encoding='latin-1') as f:
        next(f) # Pula cabeçalho
        return {f'{ano}-{mes}-{dia}T{hr}:{_min}:00Z': int(_id)
                for _id, ano, mes, dia, hr, _min in map(lambda l: l.strip().split(','), f)}
