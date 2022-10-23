package io.github.albertus82.filestore;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.List;

import org.springframework.core.io.Resource;

@SuppressWarnings("java:S1130") // Remove the declaration of thrown exception 'java.nio.file.NoSuchFileException' which is a subclass of 'java.io.IOException'. "throws" declarations should not be superfluous (java:S1130)
public interface SimpleFileStore {

	List<Resource> list(String... patterns) throws IOException;

	Resource get(String fileName) throws NoSuchFileException, IOException;

	void store(Resource resource, String fileName) throws FileAlreadyExistsException, IOException;

	void rename(String oldFileName, String newFileName) throws NoSuchFileException, FileAlreadyExistsException, IOException;

	void delete(String fileName) throws NoSuchFileException, IOException;

}
