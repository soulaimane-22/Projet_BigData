CREATE TABLE IF NOT EXISTS alerts (
    id          SERIAL PRIMARY KEY,
    timestamp   TIMESTAMP DEFAULT NOW(),
    protocol    VARCHAR(10),
    service     VARCHAR(50),
    flag        VARCHAR(10),
    src_bytes   INTEGER,
    dst_bytes   INTEGER,
    label       VARCHAR(50),
    serror_rate FLOAT,
    rerror_rate FLOAT,
    rule        VARCHAR(100)
);
