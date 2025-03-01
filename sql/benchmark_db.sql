CREATE SEQUENCE keywords_seq START 1;

CREATE TABLE keywords (
    keyword VARCHAR(40) UNIQUE NOT NULL,
    seq_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('keywords_seq'),
    count INTEGER DEFAULT 1
);

CREATE UNIQUE INDEX keyword_index ON keywords (keyword);
