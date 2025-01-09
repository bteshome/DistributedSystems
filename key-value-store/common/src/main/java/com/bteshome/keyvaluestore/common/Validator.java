package com.bteshome.keyvaluestore.common;

import java.util.UUID;

public class Validator {
    public static String notEmpty(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null.");
        }

        value = value.trim();

        if (value.isBlank()) {
            throw new IllegalArgumentException("Value cannot be empty.");
        }

        return value;
    }

    public static int positive(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Value must be greater than zero.");
        }
        return value;
    }

    public static int nonNegative(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("Value cannot be negative.");
        }
        return value;
    }

    public static int inRange(int value, int min, int max) {
        if (value < min || value > max) {
            throw new IllegalArgumentException("Value must be in the range %s - %s.".formatted(min, max));
        }
        return value;
    }

    public static int notGreaterThan(int value, int max) {
        if (value > max) {
            throw new IllegalArgumentException("Value must not be greater than %s.".formatted(max));
        }
        return value;
    }

    public static void notEqual(int value1, int value2) {
        notEqual(value1, value2, "Values %s and %s cannot be equal.".formatted(value1, value2));
    }

    public static void notEqual(int value1, int value2, String message) {
        if (value1 == value2) {
            throw new IllegalArgumentException(message);
        }
    }

    public static String setDefault(String value, String defaultValue) {
        if (value == null) {
            value = defaultValue;
        }

        value = value.trim();

        if (value.isBlank()) {
            value = defaultValue;
        }

        return value;
    }

    public static long setDefault(long value, long defaultValue) {
        if (value < 1) {
            value = defaultValue;
        }
        return value;
    }

    public static int setDefault(int value, int defaultValue) {
        if (value < 1) {
            value = defaultValue;
        }
        return value;
    }

    public static UUID setDefault(UUID value) {
        if (value == null) {
            value = UUID.randomUUID();
        }
        return value;
    }
}
