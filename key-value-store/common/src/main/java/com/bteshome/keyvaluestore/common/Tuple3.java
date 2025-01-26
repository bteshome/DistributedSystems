package com.bteshome.keyvaluestore.common;

public record Tuple3<T1, T2, T3>(T1 first, T2 second, T3 third) {
    @Override
    public String toString() {
        return "%s-%s-%s".formatted(first, second, third);
    }

    public static <T1, T2, T3> Tuple3<T1, T2, T3> of(T1 first, T2 second, T3 third) {
        return new Tuple3<>(first, second, third);
    }
}
