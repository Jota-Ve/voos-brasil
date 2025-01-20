
import enum


class ColunasDataset(enum.IntEnum):

    @staticmethod
    def _generate_next_value_(name, start, count, last_values): return count # count começa em 0


    VOOS = enum.auto()
    COMPANHIA_AEREA = enum.auto()
    CODIGO_TIPO_LINHA = enum.auto()
    PARTIDA_PREVISTA = enum.auto()
    PARTIDA_REAL = enum.auto()
    CHEGADA_PREVISTA = enum.auto()
    CHEGADA_REAL = enum.auto()
    SITUACAO_VOO = enum.auto()
    JUSTIFICATIVA = enum.auto()
    AEROPORTO_ORIGEM = enum.auto()
    CIDADE_ORIGEM = enum.auto()
    ESTADO_ORIGEM = enum.auto()
    PAIS_ORIGEM = enum.auto()
    AEROPORTO_DESTINO = enum.auto()
    CIDADE_DESTINO = enum.auto()
    ESTADO_DESTINO = enum.auto()
    PAIS_DESTINO = enum.auto()
    LONG_DEST = enum.auto()
    LAT_DEST = enum.auto()
    LONG_ORIG = enum.auto()
    LAT_ORIG = enum.auto()