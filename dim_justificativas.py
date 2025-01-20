from colunas_dataset import ColunasDataset

PATH_DIM_JUSTIFICATIVAS = 'BrFlights2/DimJustificativas.csv'

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


def map_justificativa_to_id(path=PATH_DIM_JUSTIFICATIVAS) -> dict[str, int]:
    with open(path, encoding='latin-1') as f:
        next(f)
        return {justificativa: int(_id) for _id, justificativa in map(lambda l: l.strip().split(','), f)}
