DROP SCHEMA IF EXISTS DV;

CREATE SCHEMA DV;

CREATE TABLE DV.TABLE1
(
    BOOL       BOOLEAN,
    INT1       INT DEFAULT 100,
    INT2       INTEGER,
    TINY       TINYINT,
    SMALL      SMALLINT,
    BIT_       BIT,
    NUM1       NUMERIC,
    NUM2       NUMERIC(10),
    NUM3       NUMERIC(17, 2),
    DEC1       DECIMAL,
    DEC2       DECIMAL(12),
    DEC3       DECIMAL(15, 4),
    BIG        BIGINT,
    FLOAT1     FLOAT,
    FLOAT2     FLOAT(10),
    FLOAT3     DOUBLE,
    REAL_      REAL,
    CHR1       CHAR,
    CHR2       CHAR(5),
    CHR3       CHARACTER,
    CHR4       CHARACTER(10),
    STRING1    VARCHAR(500),
    STRING2    VARCHAR(50000000),
    DATE_      DATE,
    DATETIME_  DATETIME,
    TIME_      TIME,
    TIMESTAMP_ TIMESTAMP,
    CLOB1      CLOB,
    CLOB2      CLOB(10000),
    BLOB1      BLOB,
    BLOB2      BLOB(10000),
    BIN        BINARY,
    VARBIN     VARBINARY(10000)
);

COMMENT ON TABLE DV.TABLE1 IS 'Table 1 comment';

COMMENT ON COLUMN DV.TABLE1.INT1 IS 'Integer field for Table 1';

CREATE TABLE DV.TABLE2
(
    C1 INTEGER,
    C2 INTEGER,
    C3 INTEGER
        CONSTRAINT TAB2_CHECK CHECK (C3 IS NOT NULL)
);

ALTER TABLE DV.TABLE2
    ADD CONSTRAINT TAB2_PK PRIMARY KEY (C1, C2);

CREATE TABLE DV.TABLE3
(
    X1 INTEGER,
    X2 INTEGER,
    X3 INTEGER
);

ALTER TABLE DV.TABLE3
    ADD CONSTRAINT TAB3_FK FOREIGN KEY (X1, X2) REFERENCES DV.TABLE2 (C1, C2);

CREATE TABLE DV.TABLE4
(
    C1 INT,
    C2 INT,
    C3 INT
);

CREATE INDEX DV.T4_INDEX
    ON DV.TABLE4 (C1, C2);

CREATE UNIQUE INDEX DV.T4_UNIQUE
    ON DV.TABLE4 (C3);