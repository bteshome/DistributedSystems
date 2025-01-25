package com.bteshome.keyvaluestore.common;

public record Tuple<T1, T2>(T1 first, T2 second) {
    @Override
    public String toString() {
        return "%s-%s".formatted(first, second);
    }

    public static <T1, T2> Tuple<T1, T2> of(T1 key, T2 value) {
        return new Tuple<>(key, value);
    }
}
