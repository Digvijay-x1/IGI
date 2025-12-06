-- data/init.sql
CREATE TABLE IF NOT EXISTS documents (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    status VARCHAR(20) DEFAULT 'pending', -- pending, crawled, error
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_path TEXT, -- Path in shared volume
    "offset" BIGINT, -- Byte offset in the file
    length BIGINT, -- Length of the compressed record
    doc_length INT DEFAULT 0, -- Number of words in the document
    content_hash VARCHAR(64) -- To detect duplicates
);

CREATE INDEX idx_url ON documents(url);
CREATE INDEX idx_status ON documents(status);