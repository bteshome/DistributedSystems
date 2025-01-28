package com.bteshome.keyvaluestore.common;

public record Tuple4<T1, T2, T3, T4>(T1 first, T2 second, T3 third, T4 fourth) {
    @Override
    public String toString() {
        return "%s-%s-%s-%s".formatted(first, second, third, fourth);
    }

    public static <T1, T2, T3, T4> Tuple4<T1, T2, T3, T4> of(T1 first, T2 second, T3 third, T4 fourth) {
        return new Tuple4<>(first, second, third, fourth);
    }
}
