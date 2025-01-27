package com.bteshome.keyvaluestore.common;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

@Slf4j
public class Utils {
    public static void deleteDirectoryIfItExists(String directory) {
        Path directoryToDelete = Paths.get(directory);

        try {
            if (!Files.exists(directoryToDelete))
                return;

            Files.walkFileTree(directoryToDelete, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Error deleting directory '%s'.".formatted(directory), e);
        }
    }

    public static BufferedWriter createWriter(String fileName) {
        try {
            return new BufferedWriter(new FileWriter(fileName, false));
        } catch (IOException e) {
            String errorMessage = "Error creating writer for file '%s'.".formatted(fileName);
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    public static BufferedReader createReader(String fileName) {
        try {
            return new BufferedReader(new FileReader(fileName));
        } catch (IOException e) {
            String errorMessage = "Error creating reader for file '%s'.".formatted(fileName);
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }
}
