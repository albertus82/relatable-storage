CREATE TABLE storage (
    filename         VARCHAR(255) NOT NULL PRIMARY KEY,
    content_length   NUMERIC(19, 0) /* NOT NULL DEFERRABLE INITIALLY DEFERRED */ CHECK (content_length >= 0),
    last_modified    TIMESTAMP NOT NULL,
    file_contents    BLOB NOT NULL,
    encrypt_params   VARCHAR(64),
    creation_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
