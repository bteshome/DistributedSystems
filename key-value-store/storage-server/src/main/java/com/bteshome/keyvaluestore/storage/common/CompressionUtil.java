package com.bteshome.keyvaluestore.storage.common;

import com.bteshome.keyvaluestore.common.SerDeException;
import lombok.extern.slf4j.Slf4j;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.Base64;

@Slf4j
public class CompressionUtil {
    public static void compressAndWrite(String fileName, Object object) {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
             FileOutputStream fileStream = new FileOutputStream(fileName)) {
            objectStream.writeObject(object);
            byte[] bytes = byteStream.toByteArray();
            byte[] compressedBytes = Snappy.compress(bytes);
            fileStream.write(compressedBytes);
        } catch (Exception e) {
            String errorMessage = "Error writing to file '%s'.".formatted(fileName);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public static <T> T readAndDecompress(String fileName, Class<T> clazz) {
        try {
            byte[] compressedBytes;

            try (FileInputStream fileStream = new FileInputStream(fileName);) {
                compressedBytes = fileStream.readAllBytes();
            }

            byte[] bytes = Snappy.uncompress(compressedBytes);
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
                 ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
                return clazz.cast(objectStream.readObject());
            }
        } catch (Exception e) {
            String errorMessage = "Error reading from file '%s'.".formatted(fileName);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public static byte[] readAndDecompress(String fileName) {
        try {
            byte[] compressedBytes;

            try (FileInputStream fileStream = new FileInputStream(fileName);) {
                compressedBytes = fileStream.readAllBytes();
            }

            return Snappy.uncompress(compressedBytes);
        } catch (Exception e) {
            String errorMessage = "Error reading from file '%s'.".formatted(fileName);
            throw new StorageServerException(errorMessage, e);
        }
    }
}