package com.bteshome.keyvaluestore.common;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Base64;

@Slf4j
public class JavaSerDe {
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