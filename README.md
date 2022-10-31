Simple JDBC Filestore
=====================
[![Build](https://github.com/albertus82/simple-jdbc-filestore/actions/workflows/build.yml/badge.svg)](https://github.com/albertus82/simple-jdbc-filestore/actions)
[![Known Vulnerabilities](https://snyk.io/test/github/albertus82/simple-jdbc-filestore/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/albertus82/simple-jdbc-filestore?targetFile=pom.xml)

Basic RDBMS-based filestore with encryption support.

```sql
CREATE TABLE storage (
    filename         VARCHAR(255) NOT NULL PRIMARY KEY,
    content_length   NUMERIC(19, 0) /* NOT NULL DEFERRABLE INITIALLY DEFERRED */ CHECK (content_length >= 0),
    last_modified    TIMESTAMP NOT NULL,
    file_contents    BLOB NOT NULL,
    compressed       NUMERIC(1, 0) NOT NULL CHECK (compressed IN (0, 1)),
    sha256_base64    VARCHAR(43) /* NOT NULL DEFERRABLE INITIALLY DEFERRED */,
    iv_salt_base64   VARCHAR(64),
    creation_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
```

| FILENAME | CONTENT_LENGTH | LAST_MODIFIED           | FILE_CONTENTS | COMPRESSED | SHA256_BASE64                               | IV_SALT_BASE64                                                   | CREATION_TIME           |
| -------- | -------------: | ----------------------- | ------------- | ---------: | ------------------------------------------- | ---------------------------------------------------------------- | ----------------------- |
| foo.txt  |            123 | 2022-10-31 23:10:22,607 | (BLOB)        |          1 | GW/J1z9qSb/kQMmt8W8QrmMRReQJebyJy30jzHDMrWQ | (null)                                                           | 2022-10-31 23:10:22,610 |
| bar.png  |           4567 | 2022-10-31 23:10:49,669 | (BLOB)        |          0 | mpAEA6wxO6J6G8gfCTJlK4Ag2sksI02Y+gsGvwBA7P0 | (null)                                                           | 2022-10-31 23:10:49,672 |
| baz.zip  |          89012 | 2022-10-31 23:11:02,607 | (BLOB)        |          0 | mpAEA6wxO6J6G8gfCTJlK4Ag2sksI02Y+gsGvwBA7P0 | JilvD8V8BQ3vYs3E3htFjq1sKOiKeyidjMGtc4QaxyB2hGAN3yvq7LU7Vww2sRtC | 2022-10-31 23:11:02,610 |
