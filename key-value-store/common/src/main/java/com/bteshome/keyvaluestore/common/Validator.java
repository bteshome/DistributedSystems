package com.bteshome.keyvaluestore.common;

import java.math.BigDecimal;
import java.util.UUID;

public class Validator {
    public static String notEmpty(String value) {
        return notEmpty(value, "Value");
    }

    public static String notEmpty(String value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " cannot be null.");
        }

        value = value.trim();

        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be empty.");
        }

        return value;
    }

    public static int positive(int value) {
        return positive(value, "Value");
    }

    public static int positive(int value, String  fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than zero.");
        }
        return value;
    }

    public static int nonNegative(int value) {
        return nonNegative(value, "Value");
    }

    public static int nonNegative(int value, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " cannot be negative.");
        }
        return value;
    }

    public static BigDecimal nonNegative(BigDecimal value) {
        return nonNegative(value, "Value");
    }

    public static BigDecimal nonNegative(BigDecimal value, String fieldName) {
        if (value.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException(fieldName + " cannot be negative.");
        }
        return value;
    }

    public static int inRange(int value, int min, int max) {
        return inRange(value, min, max, "Value");
    }

    public static int inRange(int value, int min, int max, String fieldName) {
        if (value < min || value > max) {
            throw new IllegalArgumentException("%s must be in the range %s - %s.".formatted(fieldName, min, max));
        }
        return value;
    }

    public static int notGreaterThan(int value, int max) {
        return notGreaterThan(value, max, "Value");
    }

    public static int notGreaterThan(int value, int max, String fieldName) {
        if (value > max) {
            throw new IllegalArgumentException("%s must not be greater than %s.".formatted(fieldName, max));
        }
        return value;
    }

    public static int notGreaterThan(int value, int max, String fieldName1, String fieldName2) {
        if (value > max) {
            throw new IllegalArgumentException("%s must not be greater than %s.".formatted(fieldName1, fieldName2));
        }
        return value;
    }

    public static int notLessThan(int value, int min) {
        return notLessThan(value, min, "Value");
    }

    public static int notLessThan(int value, int min, String fieldName) {
        if (value < min) {
            throw new IllegalArgumentException("%s must not be less than %s.".formatted(fieldName, min));
        }
        return value;
    }

    public static int notLessThan(int value, int min, String fieldName1, String fieldName2) {
        if (value < min) {
            throw new IllegalArgumentException("%s must not be less than %s.".formatted(fieldName1, fieldName2));
        }
        return value;
    }

    public static void notEqual(int value1, int value2, String fieldName1, String fieldName2) {
        if (value1 == value2) {
            throw new IllegalArgumentException("%s and %s cannot be equal.".formatted(fieldName1, fieldName2));
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

    public static String doesNotContain(String value, String pattern) {
        return doesNotContain(value, pattern, "Value");
    }

    public static String doesNotContain(String value, String pattern, String fieldName) {
        if (value == null) return value;

        if (value.contains(pattern)) {
            throw new IllegalArgumentException("%s has invalid content.".formatted(fieldName));
        }

        return value;
    }
}
