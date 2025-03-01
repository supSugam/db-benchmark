CREATE TABLE keywords (
    keyword TEXT UNIQUE NOT NULL,
    seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
    count INTEGER DEFAULT 1
);

CREATE UNIQUE INDEX keyword_index ON keywords (keyword);
