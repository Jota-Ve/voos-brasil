USE voos_brasil;

CREATE TABLE DimAeronave (
    id INT PRIMARY KEY,
    codigo VARCHAR(50),
    companhia VARCHAR(100)
);

CREATE TABLE DimAeroporto (
    id INT PRIMARY KEY,
    pais VARCHAR(50),
    estado VARCHAR(50),
    cidade VARCHAR(50),
    aeroporto VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
);

CREATE TABLE FatoVoo (
    idAeronave INT,
    partidaPrevista DATETIME,
    idOrigem INT,
    idDestino INT,
    tipoLinha VARCHAR(50),
    partidaReal DATETIME,
    chegadaPrevista DATETIME,
    chegadaReal DATETIME,
    situacao VARCHAR(50),
    justificativa TEXT,
    PRIMARY KEY (idAeronave, partidaPrevista),
    FOREIGN KEY (idAeronave) REFERENCES DimAeronave(id),
    FOREIGN KEY (idOrigem) REFERENCES DimAeroporto(id),
    FOREIGN KEY (idDestino) REFERENCES DimAeroporto(id)
);
