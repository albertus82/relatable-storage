Simple JDBC Filestore
=====================
[![Build](https://github.com/albertus82/simple-jdbc-filestore/actions/workflows/build.yml/badge.svg)](https://github.com/albertus82/simple-jdbc-filestore/actions)
[![Known Vulnerabilities](https://snyk.io/test/github/albertus82/simple-jdbc-filestore/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/albertus82/simple-jdbc-filestore?targetFile=pom.xml)

Basic RDBMS-based filestore with compression and encryption support.

The files are always stored internally in ZIP format (the compression level is customizable) in order to get CRC-32 check and AES encryption for free. This internal ZIP encoding is transparent for the client, so no manual unzip is needed.

```sql
CREATE TABLE storage (
    filename         VARCHAR(255) NOT NULL PRIMARY KEY,
    content_length   NUMERIC(19, 0) /* NOT NULL DEFERRABLE INITIALLY DEFERRED */ CHECK (content_length >= 0),
    last_modified    TIMESTAMP NOT NULL,
    compressed       NUMERIC(1, 0) NOT NULL CHECK (compressed IN (0, 1)),
    encrypted        NUMERIC(1, 0) NOT NULL CHECK (encrypted IN (0, 1)),
    file_contents    BLOB NOT NULL,
    creation_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
```

| FILENAME | CONTENT_LENGTH | LAST_MODIFIED           | COMPRESSED | ENCRYPTED | FILE_CONTENTS | CREATION_TIME           |
| -------- | -------------: | ----------------------- | ---------: | --------: | ------------- | ----------------------- |
| foo.txt  |            123 | 2022-10-31 23:10:22,607 |          1 |         0 | (BLOB)        | 2022-10-31 23:10:22,610 |
| bar.png  |           4567 | 2022-10-31 23:10:49,669 |          0 |         0 | (BLOB)        | 2022-10-31 23:10:49,672 |
| baz.zip  |          89012 | 2022-10-31 23:11:02,607 |          0 |         1 | (BLOB)        | 2022-10-31 23:11:02,610 |
