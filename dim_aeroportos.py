PATH_DIM_AEROPORTOS = 'BrFlights2/DimAeroportos.csv'


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


def map_aeroporto_to_id(path=PATH_DIM_AEROPORTOS) -> dict[tuple[float, float], int]:
    with open(path, encoding='latin-1') as f:
        next(f)
        return {(float(latitude), float(longitude)): int(_id) for _id, *_ , latitude, longitude in map(lambda l: l.strip().split(','), f)}
