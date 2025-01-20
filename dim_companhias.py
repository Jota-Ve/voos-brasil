from colunas_dataset import ColunasDataset

PATH_DIM_COMPANHIAS = 'BrFlights2/DimCompanhias.csv'


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


def map_companhia_to_id(path=PATH_DIM_COMPANHIAS) -> dict[str, int]:
    with open(path, encoding='latin-1') as f:
        next(f)
        return {companhia: int(_id) for _id, companhia in map(lambda l: l.strip().split(','), f)}
