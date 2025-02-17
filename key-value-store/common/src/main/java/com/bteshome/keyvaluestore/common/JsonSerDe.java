package com.bteshome.keyvaluestore.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.*;

@Slf4j
public class JsonSerDe {
    public static byte[] serializeToBytes(Object object) {
        try {
            return new ObjectMapper().writeValueAsBytes(object);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new SerDeException(e.getMessage(), e);
        }
    }

    public static String serialize(Object object) {
        try {
            return new ObjectMapper().writeValueAsString(object);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new SerDeException(e.getMessage(), e);
        }
    }

    public static <T> T deserialize(String serializedString, Class<T> tClass) {
        try {
            return new ObjectMapper().readValue(serializedString, tClass);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new SerDeException(e.getMessage(), e);
        }
    }

    public static <T> T deserialize(String serializedString, TypeReference<T> typeReference) {
        try {
            return new ObjectMapper().readValue(serializedString, typeReference);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new SerDeException(e.getMessage(), e);
        }
    }

    public static <T> T deserialize(byte[] bytes, Class<T> tClass) {
        try {
            return new ObjectMapper().readValue(bytes, tClass);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new SerDeException(e.getMessage(), e);
        }
    }

    public static <T> T deserialize(byte[] bytes, TypeReference<T> typeReference) {
        try {
            return new ObjectMapper().readValue(bytes, typeReference);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new SerDeException(e.getMessage(), e);
        }
    }
}