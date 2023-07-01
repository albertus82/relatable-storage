package io.github.albertus82.storage;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.util.List;

import org.springframework.core.io.Resource;

/** Basic interface for storage operations. */
@SuppressWarnings("java:S1130") // "throws" declarations should not be superfluous
public interface StorageOperations {

	/**
	 * Gets a list of references to files in the storage.
	 *
	 * @param patterns one or more filter patterns that may also include {@code *}
	 *        and {@code ?} wildcards (optional)
	 *
	 * @return a list of resources representing the files that match the provided
	 *         patterns (or all if no pattern is provided)
	 *
	 * @throws IOException if an I/O error occurs
	 */
	List<Resource> list(String... patterns) throws IOException;

	/**
	 * Gets a reference to the specified file in the storage.
	 *
	 * @param filename the name of the desired file (unique key)
	 *
	 * @return the resource representing the requested object
	 *
	 * @throws NoSuchFileException if the specified {@code filename} does not exist
	 * @throws IOException if an I/O error occurs
	 */
	Resource get(String filename) throws NoSuchFileException, IOException;

	/**
	 * Stores a new file.
	 *
	 * @param resource the data source of the file
	 * @param filename the name of the file to create
	 * @param options options specifying how the file is opened
	 *
	 * @return the resource representing the newly created object.
	 *
	 * @throws FileAlreadyExistsException if a file with the same {@code filename}
	 *         already exists
	 * @throws IOException if an I/O error occurs
	 */
	Resource put(Resource resource, String filename, OpenOption... options) throws FileAlreadyExistsException, IOException;

	/**
	 * Moves or renames a file.
	 *
	 * @param oldFilename the current (old) file name
	 * @param newFilename the desired (new) file name
	 * @param options options specifying how the move should be done
	 *
	 * @return the resource representing the moved object.
	 *
	 * @throws NoSuchFileException if {@code oldFilename} does not exist
	 * @throws FileAlreadyExistsException if {@code newFilename} already exists
	 * @throws IOException if an I/O error occurs
	 */
	Resource move(String oldFilename, String newFilename, CopyOption... options) throws NoSuchFileException, FileAlreadyExistsException, IOException;

	/**
	 * Copies a file.
	 *
	 * @param sourceFilename the source file name
	 * @param destFilename the destination file name
	 * @param options options specifying how the copy should be done
	 *
	 * @return the resource representing the newly created object.
	 *
	 * @throws NoSuchFileException if {@code sourceFilename} does not exist
	 * @throws FileAlreadyExistsException if {@code destFilename} already exists
	 * @throws IOException if an I/O error occurs
	 */
	Resource copy(String sourceFilename, String destFilename, CopyOption... options) throws NoSuchFileException, FileAlreadyExistsException, IOException;

	/**
	 * Deletes a file.
	 *
	 * @param filename the name of the file (unique key)
	 *
	 * @throws NoSuchFileException if {@code filename} does not exist
	 * @throws IOException if an I/O error occurs
	 */
	void delete(String filename) throws NoSuchFileException, IOException;

}
