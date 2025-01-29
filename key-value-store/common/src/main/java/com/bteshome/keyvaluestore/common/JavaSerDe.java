package com.bteshome.keyvaluestore.common;

import lombok.extern.slf4j.Slf4j;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.Base64;

@Slf4j
public class JavaSerDe {
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
            throw new SerDeException(errorMessage, e);
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
            throw new SerDeException(errorMessage, e);
        }
    }

    public static String serialize(Object object) {
        String serializedString = null;

        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
            objectStream.writeObject(object);
            byte[] bytes = byteStream.toByteArray();
            serializedString = Base64.getEncoder().encodeToString(bytes);
        } catch (IOException e) {
            throw new SerDeException(e);
        }

        return serializedString;
    }

    public static <T> T deserialize(String serializedString) {
        byte[] bytes = Base64.getDecoder().decode(serializedString);
        Object object = null;

        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
             ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
            object = objectStream.readObject();
        } catch (Exception e) {
            throw new SerDeException(e);
        }

        try {
            return (T)object;
        } catch (ClassCastException e) {
            throw new SerDeException(e);
        }
    }
}