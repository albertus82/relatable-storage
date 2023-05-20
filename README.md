RelaTable Storage
=================

[![Maven Central](https://img.shields.io/maven-central/v/io.github.albertus82.storage/relatable-storage)](https://mvnrepository.com/artifact/io.github.albertus82.storage/relatable-storage)
[![Build](https://github.com/albertus82/relatable-storage/actions/workflows/build.yml/badge.svg)](https://github.com/albertus82/relatable-storage/actions)
[![Known Vulnerabilities](https://snyk.io/test/github/albertus82/relatable-storage/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/albertus82/relatable-storage?targetFile=pom.xml)

### Java library to implement a RDBMS-based file storage with compression and encryption support

* Files are always stored internally in ZIP format in order to get CRC-32 check, compression and AES encryption for free.
   * The compression level is customizable from [`NONE`](src/main/java/io/github/albertus82/storage/io/Compression.java#L9) to [`HIGH`](src/main/java/io/github/albertus82/storage/io/Compression.java#L18).
   * Compression and encryption are transparent for the client, so no manual *unzip* is needed.
   * The `CONTENT_LENGTH` value represents the *original uncompressed size* of the object, it is NOT the BLOB length.
* This store has a flat structure instead of a hierarchy, so there is no direct support for things like directories or folders, but being `FILENAME` a object key string of up to 1,024 characters with no constraints other than uniqueness, you can use common prefixes (like `foo/`, `bar/`) to organize your objects simulating a hierarchical structure. For more info, you can check the [Amazon S3 documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html) because the semantics are similar.
* This library requires **JDK 11** and depends on the [Spring Framework](https://spring.io/projects/spring-framework) but **no Spring Context is actually needed** (see the [sample Java code](#sample-java-code) below).

| FILENAME | CONTENT_LENGTH | LAST_MODIFIED           | COMPRESSED | ENCRYPTED | FILE_CONTENTS | UUID_BASE64URL           | CREATION_TIME           |
| -------- | -------------: | ----------------------- | ---------: | --------: | ------------- | ------------------------ | ----------------------- |
| foo.txt  |            123 | 2022-10-31 23:10:22,607 |          1 |         0 | (BLOB)        | `IKn6ATU7RVa-qbykef7BfQ` | 2022-10-31 23:10:22,610 |
| bar.png  |           4567 | 2022-10-31 23:10:49,669 |          0 |         0 | (BLOB)        | `2WGTuBeQTu-iS5pUccAASQ` | 2022-10-31 23:10:49,672 |
| baz.zip  |          89012 | 2022-10-31 23:11:02,607 |          0 |         1 | (BLOB)        | `S2LzZ8f5S_6e5fT_p5N0Hw` | 2022-10-31 23:11:02,610 |

## Usage

### Add the Maven dependency

```xml
<dependency>
    <groupId>io.github.albertus82.storage</groupId>
    <artifactId>relatable-storage</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Create the database table

```sql
CREATE TABLE storage (
    uuid_base64url   VARCHAR(22) NOT NULL PRIMARY KEY,
    filename         VARCHAR(1024) NOT NULL UNIQUE,
    content_length   NUMERIC(19, 0) /* NOT NULL DEFERRABLE INITIALLY DEFERRED */ CHECK (content_length >= 0),
    last_modified    TIMESTAMP NOT NULL,
    compressed       NUMERIC(1, 0) NOT NULL CHECK (compressed IN (0, 1)),
    encrypted        NUMERIC(1, 0) NOT NULL CHECK (encrypted IN (0, 1)),
    file_contents    BLOB NOT NULL,
    creation_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
```
> When not using encrypion, you can also enable BLOB deduplication, if supported by your DBMS.

### Sample Java code

```java
DataSource dataSource = new DriverManagerDataSource("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"); // replace with your connection string or connection pool
StorageOperations storage = new RelaTableStorage(new JdbcTemplate(dataSource), "STORAGE", new FileBufferedBlobExtractor()); // can be customized, see Javadoc
storage.put(new PathResource("path/to/myFile.ext"), "myStoredFile.ext"); // the second argument can be prefixed to simulate a hierarchical structure
Resource resource = storage.get("myStoredFile.ext");
byte[] bytes = resource.getInputStream().readAllBytes(); // not intended for reading input streams with large amounts of data!
```

See also [SampleCodeTest](src/test/java/io/github/albertus82/storage/jdbc/SampleCodeTest.java) for a runnable JUnit test based on this code.
