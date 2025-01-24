package com.bteshome.keyvaluestore.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Tuple<T1, T2> {
    private T1 key;
    private T2 value;

    @Override
    public String toString() {
        return "%s-%s".formatted(key, value);
    }
}
