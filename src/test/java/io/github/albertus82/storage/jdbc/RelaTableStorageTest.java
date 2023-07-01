package io.github.albertus82.storage.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.DescriptiveResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.unit.DataSize;

import io.github.albertus82.storage.StorageOperations;
import io.github.albertus82.storage.TestConfig;
import io.github.albertus82.storage.TestUtils;
import io.github.albertus82.storage.io.Compression;
import io.github.albertus82.storage.jdbc.RelaTableStorage.DatabaseResource;
import io.github.albertus82.storage.jdbc.read.BlobExtractor;
import io.github.albertus82.storage.jdbc.read.DirectBlobExtractor;
import io.github.albertus82.storage.jdbc.read.FileBufferedBlobExtractor;
import io.github.albertus82.storage.jdbc.read.MemoryBufferedBlobExtractor;
import io.github.albertus82.storage.jdbc.write.BinaryStreamProvider;
import io.github.albertus82.storage.jdbc.write.FileBufferedBinaryStreamProvider;
import io.github.albertus82.storage.jdbc.write.MemoryBufferedBinaryStreamProvider;
import io.github.albertus82.storage.jdbc.write.PipeBasedBinaryStreamProvider;

@SpringJUnitConfig(TestConfig.class)
class RelaTableStorageTest {

	private static final boolean DEBUG = false;

	private static final Logger log = Logger.getLogger(RelaTableStorageTest.class.getName());

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@BeforeAll
	static void beforeAll() {
		if (DEBUG) {
			Logger.getLogger("").setLevel(Level.FINE);
			for (final Handler h : Logger.getLogger("").getHandlers()) {
				h.setLevel(Level.FINE);
			}
		}
	}

	@AfterAll
	static void afterAll() {
		if (DEBUG) {
			Logger.getLogger("").setLevel(Level.INFO);
			for (final Handler h : Logger.getLogger("").getHandlers()) {
				h.setLevel(Level.INFO);
			}
		}
	}

	@BeforeEach
	void beforeEach() {
		new ResourceDatabasePopulator(new ClassPathResource(getClass().getPackageName().replace('.', '/') + "/table.sql")).execute(jdbcTemplate.getDataSource());
	}

	@AfterEach
	void afterEach() {
		jdbcTemplate.execute("TRUNCATE TABLE storage");
		jdbcTemplate.execute("DROP TABLE storage");
	}

	@Test
	void testDatabase1() {
		jdbcTemplate.update("INSERT INTO storage (uuid_base64url, filename, content_length, file_contents, last_modified, encrypted, compressed) VALUES (?, ?, ?, ?, ?, ?, ?)", "1234567890123456789012", "a", 1, "x".getBytes(StandardCharsets.UTF_8), new Date(), true, false);
		Assertions.assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM storage", int.class));
	}

	@Test
	void testDatabase2() {
		jdbcTemplate.update("INSERT INTO storage (uuid_base64url, filename, content_length, file_contents, last_modified, encrypted, compressed) VALUES (?, ?, ?, ?, ?, ?, ?)", "qwertyuiopasdfghjklzxc", "b", 2, "yz".getBytes(StandardCharsets.UTF_8), new Date(), false, true);
		Assertions.assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM storage", int.class));
	}

	@Test
	void testApiBehaviour() throws IOException {
		final FileBufferedBlobExtractor fbbe = new FileBufferedBlobExtractor();

		Assertions.assertThrows(NullPointerException.class, () -> new RelaTableStorage(null, null, null));
		Assertions.assertThrows(NullPointerException.class, () -> new RelaTableStorage(jdbcTemplate, null, null));
		Assertions.assertThrows(NullPointerException.class, () -> new RelaTableStorage(jdbcTemplate, "STORAGE", null));
		Assertions.assertThrows(NullPointerException.class, () -> new RelaTableStorage(jdbcTemplate, null, fbbe));
		Assertions.assertThrows(NullPointerException.class, () -> new RelaTableStorage(null, "STORAGE", null));
		Assertions.assertThrows(NullPointerException.class, () -> new RelaTableStorage(null, null, fbbe));
		Assertions.assertThrows(IllegalArgumentException.class, () -> new RelaTableStorage(jdbcTemplate, "", fbbe));

		final RelaTableStorage s1 = new RelaTableStorage(jdbcTemplate, "STORAGE", fbbe);
		Assertions.assertThrows(NullPointerException.class, () -> s1.withCompression(null));
		Assertions.assertThrows(NullPointerException.class, () -> s1.withEncryption(null));
		Assertions.assertThrows(NullPointerException.class, () -> s1.withSchema(null));
		Assertions.assertThrows(IllegalArgumentException.class, () -> s1.withEncryption(new char[0]));
		Assertions.assertThrows(IllegalArgumentException.class, () -> s1.withSchema(""));
		Assertions.assertDoesNotThrow(() -> s1.withSchema("SCHEMA"));

		final StorageOperations s2 = new RelaTableStorage(jdbcTemplate, "STORAGE", fbbe).withCompression(Compression.MEDIUM);
		Assertions.assertDoesNotThrow(() -> s2.list());
		Assertions.assertNotNull(s2.list());
		Assertions.assertThrows(NullPointerException.class, () -> s2.delete(null));
		Assertions.assertThrows(NullPointerException.class, () -> s2.get(null));
		Assertions.assertThrows(NullPointerException.class, () -> s2.move("a", null));
		Assertions.assertThrows(NullPointerException.class, () -> s2.move(null, "b"));
		Assertions.assertThrows(NullPointerException.class, () -> s2.move(null, null));
		Assertions.assertThrows(NullPointerException.class, () -> s2.move("a", "b", null));
		Assertions.assertThrows(NullPointerException.class, () -> s2.copy("a", null));
		Assertions.assertThrows(NullPointerException.class, () -> s2.copy(null, "b"));
		Assertions.assertThrows(NullPointerException.class, () -> s2.copy(null, null));
		Assertions.assertThrows(NullPointerException.class, () -> s2.copy("a", "b", null));

		final FileBufferedBlobExtractor e = new FileBufferedBlobExtractor();
		Assertions.assertThrows(NullPointerException.class, () -> e.withDirectory(null));

		final DescriptiveResource dr = new DescriptiveResource("x");
		Assertions.assertThrows(NullPointerException.class, () -> s2.put(null, "y"));
		Assertions.assertThrows(NullPointerException.class, () -> s2.put(dr, null));
		Assertions.assertThrows(NullPointerException.class, () -> s2.put(null, null));
		Assertions.assertThrows(NullPointerException.class, () -> s2.put(dr, "y", null));

		final RelaTableStorage s3 = new RelaTableStorage(jdbcTemplate, "StORaGe", fbbe);
		Assertions.assertEquals("StORaGe", s3.getTable());
		Assertions.assertEquals(Optional.empty(), s3.getSchema());
		Assertions.assertEquals(Compression.NONE, s3.getCompression());
		Assertions.assertEquals(false, s3.isAlwaysQuotedIdentifiers());
		Assertions.assertEquals("AbC", s3.withSchema("AbC").getSchema().orElseThrow());
		Assertions.assertEquals(Compression.MEDIUM, s3.withCompression(Compression.MEDIUM).getCompression());
		Assertions.assertEquals(true, s3.withAlwaysQuotedIdentifiers(true).isAlwaysQuotedIdentifiers());

		Assertions.assertThrows(UnsupportedOperationException.class, () -> s3.put(dr, "x", StandardOpenOption.APPEND));
		Assertions.assertThrows(UnsupportedOperationException.class, () -> s3.put(dr, "x", StandardOpenOption.DELETE_ON_CLOSE));
		Assertions.assertThrows(IllegalArgumentException.class, () -> s3.put(dr, "x", StandardOpenOption.READ));
	}

	@Test
	void testCustomSchema() throws IOException {
		jdbcTemplate.execute("CREATE SCHEMA qwerty");
		try {
			jdbcTemplate.execute("CREATE TABLE qwerty.asdfgh AS (SELECT * FROM storage WHERE 0=1)");
			try {
				final RelaTableStorage store = new RelaTableStorage(jdbcTemplate, "ASDFGH", new FileBufferedBlobExtractor()).withCompression(Compression.LOW).withSchema("QWERTY");
				Assertions.assertEquals(0, store.list().size());
				try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
					final Resource toSave = new InputStreamResource(is);
					store.put(toSave, "myfile.txt");
				}
				Assertions.assertEquals(1, store.list().size());
			}
			finally {
				jdbcTemplate.execute("DROP TABLE qwerty.asdfgh");
			}
		}
		finally {
			jdbcTemplate.execute("DROP SCHEMA qwerty");
		}
	}

	@Test
	void testList() throws IOException {
		final StorageOperations store = new RelaTableStorage(jdbcTemplate, "STORAGE", new FileBufferedBlobExtractor()).withCompression(Compression.HIGH).withAlwaysQuotedIdentifiers(false);
		Assertions.assertEquals(0, store.list().size());
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			final Resource toSave = new InputStreamResource(is);
			store.put(toSave, "myfile.txt");
		}
		Assertions.assertEquals(1, store.list().size());
	}

	@Test
	void testListWithFilters() throws IOException {
		final StorageOperations store = new RelaTableStorage(jdbcTemplate, "STORAGE", new FileBufferedBlobExtractor().withCompression(Compression.MEDIUM)).withAlwaysQuotedIdentifiers(true).withCompression(Compression.MEDIUM);
		Assertions.assertEquals(0, store.list().size());
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "foo.txt");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "bar.txt");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "test1.txt");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "test2.txt");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "tax%.txt");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "taxi.txt");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "file.dat");
		}
		Assertions.assertEquals(7, store.list().size());
		Assertions.assertEquals(7, store.list("*").size());
		Assertions.assertEquals(4, store.list("t*").size());
		Assertions.assertEquals(2, store.list("te*").size());
		Assertions.assertEquals(2, store.list("tes*").size());
		Assertions.assertEquals(2, store.list("test*").size());
		Assertions.assertEquals(1, store.list("tax%*").size());
		Assertions.assertEquals(1, store.list("tax%.txt").size());
		Assertions.assertEquals(2, store.list("tax*.txt").size());
		Assertions.assertEquals(2, store.list("tax?.txt").size());
		Assertions.assertEquals(2, store.list("?ax*.txt").size());
		Assertions.assertEquals(2, store.list("*ax*").size());
		Assertions.assertEquals(1, store.list("foo.txt").size());
		Assertions.assertEquals(2, store.list("foo.txt", "bar.txt").size());
		Assertions.assertEquals(2, store.list("*oo.txt", "bar.txt").size());
		Assertions.assertEquals(6, store.list("*.txt").size());
		Assertions.assertEquals(2, store.list("*oo.txt", "bar.txt").size());
		Assertions.assertEquals(6, store.list("*.txt", "bar.txt").size());
		Assertions.assertEquals(1, store.list("*.d?t").size());

		jdbcTemplate.execute("TRUNCATE TABLE storage");

		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "firstDir/foo.txt");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "firstDir/tax%.txt");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "firstDir/taxi.txt");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "firstDir/anotherDir/file.dat");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "secondDir/bar.txt");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "secondDir/test1.txt");
		}
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			store.put(new InputStreamResource(is), "secondDir/test2.txt");
		}
		Assertions.assertEquals(4, store.list("firstDir/*").size());
		Assertions.assertEquals(3, store.list("secondDir/*").size());
		Assertions.assertEquals(1, store.list("firstDir/anotherDir/*").size());
	}

	@Test
	void testMove() throws IOException {
		final StorageOperations store = new RelaTableStorage(jdbcTemplate, "STORAGE", new FileBufferedBlobExtractor()).withCompression(Compression.MEDIUM);
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			final Resource toSave1 = new InputStreamResource(is);
			store.put(toSave1, "foo.txt", StandardOpenOption.TRUNCATE_EXISTING); // insert (and replace... nothing)
		}
		// foo.txt
		final Resource storedFoo = store.get("foo.txt");
		Assertions.assertTrue(storedFoo.exists());
		final Resource bar1 = store.move("foo.txt", "bar.txt");
		// bar.txt
		Assertions.assertFalse(storedFoo.exists());
		Assertions.assertThrows(NoSuchFileException.class, () -> store.get("foo.txt"));
		final Resource bar2 = store.get("bar.txt");
		Assertions.assertTrue(bar2.exists());
		Assertions.assertEquals(storedFoo.getURI(), bar2.getURI());
		Assertions.assertEquals(storedFoo.contentLength(), bar2.contentLength());
		Assertions.assertEquals(storedFoo.lastModified(), bar2.lastModified());
		Assertions.assertNotEquals(storedFoo.getDescription(), bar2.getDescription());
		Assertions.assertEquals(bar1.getURI(), bar2.getURI());
		Assertions.assertEquals(bar1.contentLength(), bar2.contentLength());
		Assertions.assertEquals(bar1.lastModified(), bar2.lastModified());
		Assertions.assertEquals(bar1.getDescription(), bar2.getDescription());
		Assertions.assertEquals(bar1, bar2);
		try (final InputStream is = bar2.getInputStream()) {
			Assertions.assertArrayEquals("qwertyuiop".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
		Assertions.assertEquals(storedFoo.lastModified(), bar2.lastModified());
		Assertions.assertEquals(storedFoo.contentLength(), bar2.contentLength());
		Assertions.assertEquals(1, store.list().size());
		Assertions.assertThrows(NoSuchFileException.class, () -> store.move("foo.txt", "baz.txt")); // move without replace
		try (final InputStream is = new ByteArrayInputStream("asdfghjkl".getBytes(StandardCharsets.US_ASCII))) {
			final Resource toSave2 = new InputStreamResource(is);
			store.put(toSave2, "foo.txt"); // insert without replace
		}
		// foo.txt, bar.txt
		Assertions.assertEquals(2, store.list().size());
		try (final InputStream is = storedFoo.getInputStream()) {
			Assertions.assertArrayEquals("asdfghjkl".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
		Assertions.assertThrows(FileAlreadyExistsException.class, () -> store.move("foo.txt", "bar.txt"));
		// foo.txt, bar.txt
		store.move("foo.txt", "bar.txt", StandardCopyOption.REPLACE_EXISTING); // move with replace
		// bar.txt
		Assertions.assertEquals(1, store.list().size());
		try (final InputStream is = bar2.getInputStream()) {
			Assertions.assertArrayEquals("asdfghjkl".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
		Assertions.assertThrows(IllegalStateException.class, () -> store.move("bar.txt", "baz.txt", StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)); // move with replace
	}

	@Test
	@Transactional
	void testMoveTransactional() throws IOException {
		final StorageOperations store = new RelaTableStorage(jdbcTemplate, "STORAGE", new FileBufferedBlobExtractor()).withCompression(Compression.MEDIUM);
		final Resource storedFoo;
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			final Resource toSave1 = new InputStreamResource(is);
			storedFoo = store.put(toSave1, "foo.txt"); // insert (and replace... nothing)
		}
		try (final InputStream is = storedFoo.getInputStream()) {
			Assertions.assertArrayEquals("qwertyuiop".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
		try (final InputStream is = store.get("foo.txt").getInputStream()) {
			Assertions.assertArrayEquals("qwertyuiop".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
		// foo.txt
		Assertions.assertEquals(1, store.list().size());
		final Resource storedBar = store.put(new ByteArrayResource("asdfghjkl".getBytes(StandardCharsets.US_ASCII)), "bar.txt");
		// foo.txt, bar.txt
		Assertions.assertEquals(2, store.list().size());
		try (final InputStream is = storedBar.getInputStream()) {
			Assertions.assertArrayEquals("asdfghjkl".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
		try (final InputStream is = store.get("bar.txt").getInputStream()) {
			Assertions.assertArrayEquals("asdfghjkl".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
		Assertions.assertThrows(FileAlreadyExistsException.class, () -> store.move("foo.txt", "bar.txt", StandardCopyOption.ATOMIC_MOVE)); // move without replace
		Assertions.assertEquals(2, store.list().size());
		Assertions.assertDoesNotThrow(() -> store.move("foo.txt", "bar.txt", StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)); // move with replace
		// bar.txt
		Assertions.assertEquals(1, store.list().size());
		try (final InputStream is = store.get("bar.txt").getInputStream()) {
			Assertions.assertArrayEquals("qwertyuiop".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
	}

	@Test
	void testCopy() throws IOException {
		final StorageOperations store = new RelaTableStorage(jdbcTemplate, "STORAGE", new FileBufferedBlobExtractor()).withCompression(Compression.MEDIUM);
		final Resource saved1;
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			final Resource toSave1 = new InputStreamResource(is);
			saved1 = store.put(toSave1, "foo.txt"); // insert without replace
		}
		// foo.txt
		Assertions.assertTrue(store.get("foo.txt").exists());
		final Resource copied1 = store.copy("foo.txt", "bar.txt"); // copy without replace
		// foo.txt, bar.txt
		Assertions.assertTrue(store.get("bar.txt").exists());
		Assertions.assertNotEquals(saved1.getURI(), copied1.getURI());
		Assertions.assertEquals(saved1.contentLength(), copied1.contentLength());
		Assertions.assertEquals(saved1.lastModified(), copied1.lastModified());
		final Resource copied2 = store.copy("foo.txt", "aaa.txt", StandardCopyOption.REPLACE_EXISTING); // copy with replace
		// foo.txt, bar.txt, aaa.txt
		Assertions.assertTrue(store.get("aaa.txt").exists());
		Assertions.assertNotEquals(saved1.getURI(), copied2.getURI());
		Assertions.assertEquals(saved1.contentLength(), copied2.contentLength());
		Assertions.assertEquals(saved1.lastModified(), copied2.lastModified());
		Assertions.assertEquals(3, store.list().size());
		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			final Resource toSave2 = new InputStreamResource(is);
			Assertions.assertThrows(FileAlreadyExistsException.class, () -> store.put(toSave2, "foo.txt")); // insert without replace
		}
		final Resource toSave3 = new ByteArrayResource("asdfghjkl".getBytes(StandardCharsets.US_ASCII));
		final Resource savedBaz = store.put(toSave3, "baz.txt"); // insert without replace
		Assertions.assertEquals(9, savedBaz.contentLength());
		// foo.txt, bar.txt, aaa.txt, baz.txt
		try (final InputStream is2 = savedBaz.getInputStream()) {
			Assertions.assertArrayEquals("asdfghjkl".getBytes(StandardCharsets.US_ASCII), is2.readAllBytes());
		}
		try (final InputStream is = store.get("baz.txt").getInputStream()) {
			Assertions.assertArrayEquals("asdfghjkl".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
		Assertions.assertEquals(4, store.list().size());
		Assertions.assertThrows(FileAlreadyExistsException.class, () -> store.copy("baz.txt", "bar.txt"));
		try (final InputStream is = store.get("bar.txt").getInputStream()) {
			Assertions.assertArrayEquals("qwertyuiop".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
		final Resource r = store.copy("baz.txt", "bar.txt", StandardCopyOption.REPLACE_EXISTING); // copy with replace
		Assertions.assertEquals(4, store.list().size());
		try (final InputStream is = r.getInputStream()) {
			Assertions.assertArrayEquals("asdfghjkl".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
		try (final InputStream is = store.get("bar.txt").getInputStream()) {
			Assertions.assertArrayEquals("asdfghjkl".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
	}

	@Test
	void testStoreListGetDeleteFromStream() throws IOException {
		for (final BlobExtractor be : new BlobExtractor[] { new FileBufferedBlobExtractor(), new FileBufferedBlobExtractor().withCompression(Compression.NONE), new MemoryBufferedBlobExtractor(), new MemoryBufferedBlobExtractor().withCompression(Compression.MEDIUM) }) {
			for (final Compression compression : Compression.values()) {
				for (final BinaryStreamProvider bsp : new BinaryStreamProvider[] { new PipeBasedBinaryStreamProvider(), new PipeBasedBinaryStreamProvider().withPipeSize(512), new PipeBasedBinaryStreamProvider().withPipeSize(1_048_576), new FileBufferedBinaryStreamProvider(), new MemoryBufferedBinaryStreamProvider() }) {
					final StorageOperations store = new RelaTableStorage(jdbcTemplate, "STORAGE", be).withCompression(compression).withBinaryStreamProvider(bsp);
					try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
						store.put(new InputStreamResource(is), "myfile.txt");
					}
					final long timeAfter = System.currentTimeMillis();
					final List<Resource> list = store.list();
					Assertions.assertEquals(1, list.size());
					final Resource r1 = list.get(0);
					Assertions.assertEquals("qwertyuiop".length(), r1.contentLength());
					Assertions.assertTrue(r1.exists());
					try (final InputStream is = r1.getInputStream()) {
						Assertions.assertArrayEquals("qwertyuiop".getBytes(StandardCharsets.UTF_8), is.readAllBytes());
						Assertions.assertEquals("myfile.txt", r1.getFilename());
						Assertions.assertNotNull(r1.getURI());
						final String uriStr = r1.getURI().toString();
						Assertions.assertTrue(uriStr.startsWith("urn:uuid:"));
						Assertions.assertDoesNotThrow(() -> UUID.fromString(uriStr.substring(uriStr.lastIndexOf(':') + 1)));
						Assertions.assertTrue(timeAfter - r1.lastModified() < TimeUnit.SECONDS.toMillis(10));
					}
					final Resource r2 = store.get(r1.getFilename());
					Assertions.assertEquals(r1.contentLength(), r2.contentLength());
					Assertions.assertTrue(r2.exists());
					try (final InputStream is = r2.getInputStream()) {
						Assertions.assertArrayEquals("qwertyuiop".getBytes(StandardCharsets.UTF_8), is.readAllBytes());
						Assertions.assertEquals(r1.getFilename(), r2.getFilename());
						Assertions.assertEquals(r1.lastModified(), r2.lastModified());
						Assertions.assertEquals(r1.getURI(), r2.getURI());
					}
					store.delete("myfile.txt");
					Assertions.assertFalse(r2.exists());
					Assertions.assertThrows(NoSuchFileException.class, () -> r2.getInputStream());
					Assertions.assertThrows(NoSuchFileException.class, () -> store.get("myfile.txt"));
					Assertions.assertThrows(NoSuchFileException.class, () -> store.delete("myfile.txt"));
				}
			}
		}
	}

	@Test
	void testEncryptedStoreListGetDeleteFromStream() throws IOException {
		for (final BlobExtractor be : new BlobExtractor[] { new FileBufferedBlobExtractor(), new FileBufferedBlobExtractor().withCompression(Compression.MEDIUM), new MemoryBufferedBlobExtractor(), new MemoryBufferedBlobExtractor().withCompression(Compression.MEDIUM) }) {
			for (final Compression compression : Compression.values()) {
				for (final BinaryStreamProvider bsp : new BinaryStreamProvider[] { new PipeBasedBinaryStreamProvider(), new PipeBasedBinaryStreamProvider().withPipeSize(512), new PipeBasedBinaryStreamProvider().withPipeSize(1_048_576), new FileBufferedBinaryStreamProvider(), new MemoryBufferedBinaryStreamProvider() }) {
					final RelaTableStorage store = new RelaTableStorage(jdbcTemplate, "STORAGE", be).withCompression(compression).withBinaryStreamProvider(bsp).withEncryption("TestPassword0$".toCharArray());
					try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
						store.put(new InputStreamResource(is), "myfile.txt");
					}
					final long timeAfter = System.currentTimeMillis();
					final List<Resource> list = store.list();
					Assertions.assertEquals(1, list.size());
					final Resource r1 = list.get(0);
					Assertions.assertEquals("qwertyuiop".length(), r1.contentLength());
					Assertions.assertTrue(r1.exists());
					try (final InputStream is = r1.getInputStream()) {
						Assertions.assertArrayEquals("qwertyuiop".getBytes(StandardCharsets.UTF_8), is.readAllBytes());
						Assertions.assertEquals("myfile.txt", r1.getFilename());
						Assertions.assertNotNull(r1.getURI());
						final String uriStr = r1.getURI().toString();
						Assertions.assertTrue(uriStr.startsWith("urn:uuid:"));
						Assertions.assertDoesNotThrow(() -> UUID.fromString(uriStr.substring(uriStr.lastIndexOf(':') + 1)));
						Assertions.assertTrue(timeAfter - r1.lastModified() < TimeUnit.SECONDS.toMillis(10));
					}
					final Resource r2 = store.get(r1.getFilename());
					Assertions.assertEquals(r1.contentLength(), r2.contentLength());
					Assertions.assertTrue(r2.exists());
					try (final InputStream is = r2.getInputStream()) {
						Assertions.assertArrayEquals("qwertyuiop".getBytes(StandardCharsets.UTF_8), is.readAllBytes());
						Assertions.assertEquals(r1.getFilename(), r2.getFilename());
						Assertions.assertEquals(r1.lastModified(), r2.lastModified());
						Assertions.assertEquals(r1.getURI(), r2.getURI());
					}
					store.delete("myfile.txt");
					Assertions.assertFalse(r2.exists());
					Assertions.assertThrows(NoSuchFileException.class, () -> r2.getInputStream());
					Assertions.assertThrows(NoSuchFileException.class, () -> store.get("myfile.txt"));
					Assertions.assertThrows(NoSuchFileException.class, () -> store.delete("myfile.txt"));
				}
			}
		}
	}

	@Test
	void testPutListGetDeleteFromFile() throws IOException {
		for (final BlobExtractor be : new BlobExtractor[] { new FileBufferedBlobExtractor(), new FileBufferedBlobExtractor().withCompression(Compression.HIGH), new MemoryBufferedBlobExtractor(), new MemoryBufferedBlobExtractor().withCompression(Compression.LOW) }) {
			for (final Compression compression : Compression.values()) {
				for (final BinaryStreamProvider bsp : new BinaryStreamProvider[] { new PipeBasedBinaryStreamProvider(), new PipeBasedBinaryStreamProvider().withPipeSize(512), new PipeBasedBinaryStreamProvider().withPipeSize(1_048_576), new FileBufferedBinaryStreamProvider(), new MemoryBufferedBinaryStreamProvider() }) {
					final StorageOperations store = new RelaTableStorage(jdbcTemplate, "STORAGE", be).withCompression(compression).withBinaryStreamProvider(bsp);
					Path tempFile = null;
					try {
						tempFile = Files.createTempFile(null, null);
						tempFile.toFile().deleteOnExit();
						Files.writeString(tempFile, "asdfghjkl");
						final BasicFileAttributes tempFileAttr = Files.readAttributes(tempFile, BasicFileAttributes.class);
						store.put(new FileSystemResource(tempFile), "myfile.txt");
						final List<Resource> list = store.list();
						Assertions.assertEquals(1, list.size());
						final Resource r1 = list.get(0);
						Assertions.assertEquals("asdfghjkl".length(), r1.contentLength());
						Assertions.assertTrue(r1.exists());
						try (final InputStream is = r1.getInputStream()) {
							Assertions.assertArrayEquals("asdfghjkl".getBytes(StandardCharsets.UTF_8), is.readAllBytes());
							Assertions.assertEquals("myfile.txt", r1.getFilename());
							Assertions.assertNotNull(r1.getURI());
							final String uriStr = r1.getURI().toString();
							Assertions.assertTrue(uriStr.startsWith("urn:uuid:"));
							Assertions.assertDoesNotThrow(() -> UUID.fromString(uriStr.substring(uriStr.lastIndexOf(':') + 1)));
							Assertions.assertEquals(tempFileAttr.lastModifiedTime().toMillis(), r1.lastModified());
						}
						final Resource r2 = store.get(r1.getFilename());
						Assertions.assertEquals(r1.contentLength(), r2.contentLength());
						Assertions.assertTrue(r2.exists());
						try (final InputStream is = r2.getInputStream()) {
							Assertions.assertArrayEquals("asdfghjkl".getBytes(StandardCharsets.UTF_8), is.readAllBytes());
							Assertions.assertEquals(r1.getFilename(), r2.getFilename());
							Assertions.assertEquals(r1.lastModified(), r2.lastModified());
							Assertions.assertEquals(r1.getURI(), r2.getURI());
						}
						store.delete("myfile.txt");
						Assertions.assertFalse(r2.exists());
						Assertions.assertThrows(NoSuchFileException.class, () -> r2.getInputStream());
						Assertions.assertThrows(NoSuchFileException.class, () -> store.get("myfile.txt"));
						Assertions.assertThrows(NoSuchFileException.class, () -> store.delete("myfile.txt"));
					}
					finally {
						Files.deleteIfExists(tempFile);
					}
				}
			}
		}
	}

	@Test
	void testPut() throws IOException {
		final StorageOperations store = new RelaTableStorage(jdbcTemplate, "STORAGE", new FileBufferedBlobExtractor()).withCompression(Compression.LOW);
		final Resource saved1;
		try (final InputStream is = new ByteArrayInputStream("zxcvbnm".getBytes(StandardCharsets.UTF_8))) {
			final Resource toSave = new InputStreamResource(is);
			Assertions.assertThrows(IllegalArgumentException.class, () -> store.put(toSave, "myfile.txt", StandardOpenOption.READ));
			Assertions.assertThrows(UnsupportedOperationException.class, () -> store.put(toSave, "myfile.txt", StandardOpenOption.DELETE_ON_CLOSE));
			Assertions.assertThrows(UnsupportedOperationException.class, () -> store.put(toSave, "myfile.txt", StandardOpenOption.APPEND));
			saved1 = store.put(toSave, "myfile.txt");
		}
		// myfile.txt
		final long lm = saved1.lastModified();
		Assertions.assertEquals(1, store.list().size());
		Assertions.assertTrue(lm > System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(60));
		Assertions.assertEquals(7, saved1.contentLength());
		Assertions.assertEquals("myfile.txt", saved1.getFilename());
		final URI uri = saved1.getURI();
		Assertions.assertTrue(uri.toString().startsWith("urn:uuid:"));
		try (final InputStream is = saved1.getInputStream()) {
			Assertions.assertArrayEquals("zxcvbnm".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}
		final Resource retrieved1 = store.get("myfile.txt");
		Assertions.assertEquals(1, store.list().size());
		Assertions.assertEquals(retrieved1.lastModified(), lm);
		Assertions.assertEquals(retrieved1.contentLength(), saved1.contentLength());
		Assertions.assertEquals(retrieved1.getFilename(), saved1.getFilename());
		Assertions.assertEquals(retrieved1.getURI(), saved1.getURI());
		try (final InputStream is = retrieved1.getInputStream()) {
			Assertions.assertArrayEquals("zxcvbnm".getBytes(StandardCharsets.US_ASCII), is.readAllBytes());
		}

		try (final InputStream is = getClass().getResourceAsStream("10b.txt")) {
			Assertions.assertThrows(FileAlreadyExistsException.class, () -> store.put(new InputStreamResource(is), "myfile.txt"));
		}
		final Resource saved2 = store.put(new ByteArrayResource("qwertyuiop".getBytes(StandardCharsets.US_ASCII)), "myfile.txt", StandardOpenOption.TRUNCATE_EXISTING); // replace
		try (final InputStream is2 = saved2.getInputStream()) {
			Assertions.assertArrayEquals("qwertyuiop".getBytes(StandardCharsets.US_ASCII), is2.readAllBytes());
		}
		Assertions.assertNotNull(saved2.getURI());
		Assertions.assertNotEquals(0, saved2.lastModified());
		Assertions.assertEquals("myfile.txt", saved2.getFilename());
		Assertions.assertEquals(10, saved2.contentLength());
		final Resource retrieved = store.get("myfile.txt");
		Assertions.assertEquals(saved2.getFilename(), retrieved.getFilename());
		Assertions.assertEquals(saved2.lastModified(), retrieved.lastModified());
		Assertions.assertEquals(saved2.contentLength(), retrieved.contentLength());
		Assertions.assertEquals(saved2.getURI(), retrieved.getURI());
	}

	@Test
	void testPutLargeParallel() throws Exception {
		final DataSize fileSize = DataSize.ofMegabytes(16);
		Path tempFile = null;
		try {
			tempFile = TestUtils.createDummyFile(fileSize);
			// Compute SHA-256 of the dummy file
			final byte[] buffer0 = new byte[8192];
			final MessageDigest digestSource = MessageDigest.getInstance("SHA-256");
			try (final InputStream is = Files.newInputStream(tempFile)) {
				int bytesCount = 0;
				while ((bytesCount = is.read(buffer0)) != -1) {
					digestSource.update(buffer0, 0, bytesCount);
				}
			}
			final byte[] sha256Source = digestSource.digest();

			final Path f = tempFile;
			List.of(new FileBufferedBlobExtractor(), new FileBufferedBlobExtractor().withCompression(Compression.LOW), new MemoryBufferedBlobExtractor(), new MemoryBufferedBlobExtractor().withCompression(Compression.HIGH)).parallelStream().forEach(be -> {
				try {
					for (final Compression compression : Compression.values()) {
						for (final BinaryStreamProvider bsp : new BinaryStreamProvider[] { new PipeBasedBinaryStreamProvider(), new PipeBasedBinaryStreamProvider().withPipeSize(512), new PipeBasedBinaryStreamProvider().withPipeSize(1_048_576), new FileBufferedBinaryStreamProvider(), new MemoryBufferedBinaryStreamProvider() }) {
							final String filename = UUID.randomUUID().toString();
							final RelaTableStorage store = new RelaTableStorage(jdbcTemplate, "STORAGE", be).withCompression(compression).withBinaryStreamProvider(bsp).withEncryption("testpass".toCharArray());
							try (final InputStream is = Files.newInputStream(f)) {
								Assertions.assertDoesNotThrow(() -> store.put(new InputStreamResource(is), filename));
							}

							final byte[] buffer = new byte[8192];
							final MessageDigest digestStored = MessageDigest.getInstance("SHA-256");
							final DatabaseResource dr = store.get(filename);
							try (final InputStream stored = dr.getInputStream()) {
								int bytesCount = 0;
								while ((bytesCount = stored.read(buffer)) != -1) {
									digestStored.update(buffer, 0, bytesCount);
								}
							}
							final byte[] sha256Stored = digestStored.digest();
							Assertions.assertArrayEquals(sha256Source, sha256Stored);
							Assertions.assertNotNull(dr.getUUID());
							Assertions.assertNotNull(dr.getURI());
							Assertions.assertEquals(fileSize.toBytes(), dr.contentLength());
							final String uriStr = dr.getURI().toString();
							Assertions.assertTrue(uriStr.startsWith("urn:uuid:"));
							Assertions.assertEquals(dr.getUUID(), UUID.fromString(uriStr.substring(uriStr.lastIndexOf(':') + 1)));
						}
					}
				}
				catch (final IOException e) {
					throw new UncheckedIOException(e);
				}
				catch (final NoSuchAlgorithmException e) {
					throw new RuntimeException(e);
				}
			});
		}
		finally {
			TestUtils.deleteIfExists(tempFile);
		}
	}

	@Test
	@Transactional
	void testPutLargeTransactional() throws Exception {
		final DataSize fileSize = DataSize.ofMegabytes(16);
		Path tempFile = null;
		try {
			tempFile = TestUtils.createDummyFile(fileSize);
			// Compute SHA-256 of the dummy file
			final byte[] buffer0 = new byte[8192];
			final MessageDigest digestSource = MessageDigest.getInstance("SHA-256");
			try (final InputStream is = Files.newInputStream(tempFile)) {
				int bytesCount = 0;
				while ((bytesCount = is.read(buffer0)) != -1) {
					digestSource.update(buffer0, 0, bytesCount);
				}
			}
			final byte[] sha256Source = digestSource.digest();

			for (final Compression compression : Compression.values()) {
				final String filename = UUID.randomUUID().toString();
				final RelaTableStorage store = new RelaTableStorage(jdbcTemplate, "STORAGE", new DirectBlobExtractor()).withCompression(compression);
				try (final InputStream is = Files.newInputStream(tempFile)) {
					Assertions.assertDoesNotThrow(() -> store.put(new InputStreamResource(is), filename));
				}

				final byte[] buffer = new byte[8192];
				final MessageDigest digestStored = MessageDigest.getInstance("SHA-256");
				final DatabaseResource dr = store.get(filename);
				try (final InputStream stored = dr.getInputStream()) {
					int bytesCount = 0;
					while ((bytesCount = stored.read(buffer)) != -1) {
						digestStored.update(buffer, 0, bytesCount);
					}
				}
				final byte[] sha256Stored = digestStored.digest();
				Assertions.assertArrayEquals(sha256Source, sha256Stored);
				Assertions.assertEquals(filename, dr.getFilename());
				Assertions.assertNotNull(dr.getUUID());
				Assertions.assertNotNull(dr.getURI());
				Assertions.assertEquals(fileSize.toBytes(), dr.contentLength());
				final String uriStr = dr.getURI().toString();
				Assertions.assertTrue(uriStr.startsWith("urn:uuid:"));
				Assertions.assertEquals(dr.getUUID(), UUID.fromString(uriStr.substring(uriStr.lastIndexOf(':') + 1)));
			}
		}
		finally {
			TestUtils.deleteIfExists(tempFile);
		}
	}

	@Test
	void testReproducibleBlobs() throws Exception {
		short i = 0;
		for (final Compression c : Compression.values()) {
			final byte iterations = 2;
			final byte[] magic = "PK".getBytes(StandardCharsets.US_ASCII);

			final StorageOperations store = new RelaTableStorage(jdbcTemplate, "STORAGE", new MemoryBufferedBlobExtractor()).withCompression(c);

			final ByteArrayResource r0 = new ByteArrayResource(TestUtils.createDummyByteArray(DataSize.ofMegabytes(1)));

			final Set<Resource> resources = new LinkedHashSet<>();
			for (byte j = 0; j < iterations; j++) {
				log.log(Level.INFO, "PUT {0} {1}", new Object[] { i, c });
				resources.add(store.put(r0, "" + ++i));
				TimeUnit.SECONDS.sleep(2); // NOSONAR the built-in timestamp resolution of files in a .ZIP archive is two seconds
			}
			Assertions.assertEquals(iterations, resources.size());

			final List<byte[]> blobs = new ArrayList<>();
			for (final Resource r : resources) {
				blobs.add(jdbcTemplate.queryForObject("select FILE_CONTENTS from STORAGE where FILENAME=?", byte[].class, r.getFilename()));
			}
			Assertions.assertEquals(iterations, blobs.size());
			for (final byte[] blob : blobs) {
				log.log(Level.INFO, "SIZE {0} {1}", new Object[] { c, blob.length });
				Assertions.assertArrayEquals(magic, Arrays.copyOf(blob, magic.length));
			}

			for (byte j = 0; j < iterations - 1; j++) {
				log.log(Level.INFO, "CMP {0}", j);
				Assertions.assertArrayEquals(blobs.get(j), blobs.get(j + 1));
			}
		}
	}

}
