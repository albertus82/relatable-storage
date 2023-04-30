CREATE TABLE storage (
    uuid_b64url      VARCHAR(22) NOT NULL PRIMARY KEY,
    filename         VARCHAR(1024) NOT NULL UNIQUE,
    content_length   NUMERIC(19, 0) /* NOT NULL DEFERRABLE INITIALLY DEFERRED */ CHECK (content_length >= 0),
    last_modified    TIMESTAMP NOT NULL,
    compressed       NUMERIC(1, 0) NOT NULL CHECK (compressed IN (0, 1)),
    encrypted        NUMERIC(1, 0) NOT NULL CHECK (encrypted IN (0, 1)),
    file_contents    BLOB NOT NULL,
    creation_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
