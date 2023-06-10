package io.github.albertus82.storage.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.PathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import io.github.albertus82.storage.StorageOperations;
import io.github.albertus82.storage.TestUtils;
import io.github.albertus82.storage.jdbc.read.FileBufferedBlobExtractor;

class SampleCodeTest {

	private static final String jdbcUrl = "jdbc:h2:mem:" + SampleCodeTest.class.getSimpleName() + ";DB_CLOSE_DELAY=-1";
	private static final byte[] tempFileContents = "1234567890".getBytes(StandardCharsets.US_ASCII);
	private static Path tempFile;

	@BeforeAll
	static void beforeAll() throws IOException {
		tempFile = Files.createTempFile(null, null);
		Files.write(tempFile, tempFileContents);
		var dataSource = new DriverManagerDataSource(jdbcUrl);
		new ResourceDatabasePopulator(new ClassPathResource(SampleCodeTest.class.getPackageName().replace('.', '/') + "/table.sql")).execute(dataSource);
	}

	@AfterAll
	static void afterAll() {
		TestUtils.deleteIfExists(tempFile);
	}

	@Test
	void test() throws IOException {
		DataSource dataSource = new DriverManagerDataSource(jdbcUrl);
		StorageOperations store = new RelaTableStorage(new JdbcTemplate(dataSource), "STORAGE", new FileBufferedBlobExtractor());
		store.put(new PathResource(tempFile), "myStoredFile.ext");
		Resource resource = store.get("myStoredFile.ext");
		try (InputStream in = resource.getInputStream()) {
			byte[] bytes = in.readAllBytes();
			Assertions.assertArrayEquals(tempFileContents, bytes);
		}
	}

}
