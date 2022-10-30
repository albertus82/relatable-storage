CREATE TABLE storage (
    filename         VARCHAR(255) NOT NULL PRIMARY KEY,
    content_length   NUMERIC(19, 0) /* NOT NULL DEFERRABLE INITIALLY DEFERRED */ CHECK (content_length >= 0),
    last_modified    TIMESTAMP NOT NULL,
    file_contents    BLOB NOT NULL,
    compressed       NUMERIC(1, 0) NOT NULL CHECK (compressed IN (0, 1)),
    iv_salt_base64   VARCHAR(64),
    sha256_base64    VARCHAR(43) /* NOT NULL DEFERRABLE INITIALLY DEFERRED */,
    creation_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
