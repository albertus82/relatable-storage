Simple JDBC Filestore
=====================
[![Build](https://github.com/albertus82/simple-jdbc-filestore/actions/workflows/build.yml/badge.svg)](https://github.com/albertus82/simple-jdbc-filestore/actions)
[![Known Vulnerabilities](https://snyk.io/test/github/albertus82/simple-jdbc-filestore/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/albertus82/simple-jdbc-filestore?targetFile=pom.xml)

Basic RDBMS-based filestore

```sql
CREATE TABLE storage (
    filename         VARCHAR(255) NOT NULL PRIMARY KEY,
    content_length   NUMERIC(19, 0) /* NOT NULL DEFERRABLE INITIALLY DEFERRED */ CHECK (content_length >= 0),
    last_modified    TIMESTAMP NOT NULL,
    file_contents    BLOB NOT NULL,
    compressed       NUMERIC(1, 0) NOT NULL CHECK (compressed IN (0, 1)),
    sha256_base64    VARCHAR(43) /* NOT NULL DEFERRABLE INITIALLY DEFERRED */,
    creation_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
```

| FILENAME | CONTENT_LENGTH | LAST_MODIFIED          | FILE_CONTENTS                        | COMPRESSED | SHA256_BASE64                               | CREATION_TIME          |
| -------- | -------------: | ---------------------- | ------------------------------------ | ---------: | ------------------------------------------- | ---------------------- |
| foo.txt  |              9 | 26-JUN-22 08:40:46,907 | 6173646667686a6b6c                   |          0 | GW/J1z9qSb/kQMmt8W8QrmMRReQJebyJy30jzHDMrWQ | 26-JUN-22 08:40:46,985 |
| bar.txt  |             10 | 26-JUN-22 08:33:31,035 | 789c2b2c4f2d2aa92ccdcc2f0000185b046a |          1 | mpAEA6wxO6J6G8gfCTJlK4Ag2sksI02Y+gsGvwBA7P0 | 26-JUN-22 08:33:31,036 |
