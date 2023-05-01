package io.github.albertus82.filestore;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.List;

import org.springframework.core.io.Resource;

/** Basic interface for filestore operations. */
@SuppressWarnings("java:S1130") // "throws" declarations should not be superfluous
public interface SimpleFileStore {

	/**
	 * Gets a list of references to files in the store.
	 *
	 * @param patterns one or more filter patterns that may also include {@code *}
	 *        and {@code ?} wildcards (optional)
	 *
	 * @return a list of objects referencing the files matching the provided pattern
	 *         (or all if no pattern is provided)
	 *
	 * @throws IOException if an I/O error occurs
	 */
	List<Resource> list(String... patterns) throws IOException;

	/**
	 * Gets a reference to the specified file in the store.
	 *
	 * @param fileName the name of the desired file (unique key)
	 *
	 * @return an object referencing the requested file
	 *
	 * @throws NoSuchFileException if the specified {@code fileName} does not exist
	 * @throws IOException if an I/O error occurs
	 */
	Resource get(String fileName) throws NoSuchFileException, IOException;

	/**
	 * Stores a new file.
	 *
	 * @param resource the data source of the file
	 * @param fileName the name of the file to create
	 *
	 * @return the resource representing the newly created object.
	 *
	 * @throws FileAlreadyExistsException if a file with the same {@code fileName}
	 *         already exists
	 * @throws IOException if an I/O error occurs
	 */
	Resource put(Resource resource, String fileName) throws FileAlreadyExistsException, IOException;

	/**
	 * Moves or renames a file.
	 *
	 * @param oldFileName the current (old) file name
	 * @param newFileName the desired (new) file name
	 *
	 * @throws NoSuchFileException if {@code oldFileName} does not exist
	 * @throws FileAlreadyExistsException if {@code newFileName} already exists
	 * @throws IOException if an I/O error occurs
	 */
	void move(String oldFileName, String newFileName) throws NoSuchFileException, FileAlreadyExistsException, IOException;

	/**
	 * Copies a file.
	 *
	 * @param sourceFileName the source file name
	 * @param destFileName the destination file name
	 *
	 * @return the resource representing the newly created object.
	 *
	 * @throws NoSuchFileException if {@code sourceFileName} does not exist
	 * @throws FileAlreadyExistsException if {@code destFileName} already exists
	 * @throws IOException if an I/O error occurs
	 */
	Resource copy(String sourceFileName, String destFileName) throws NoSuchFileException, FileAlreadyExistsException, IOException;

	/**
	 * Deletes a file.
	 *
	 * @param fileName the name of the file (unique key)
	 *
	 * @throws NoSuchFileException if {@code fileName} does not exist
	 * @throws IOException if an I/O error occurs
	 */
	void delete(String fileName) throws NoSuchFileException, IOException;

}
