package com.bteshome.keyvaluestore.storage.entities;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public record ItemKey(String keyString, long expiryTime) implements Serializable {
    public static ItemKey of(String keyString, long expiryTime) {
        return new ItemKey(keyString, expiryTime);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ItemKey itemKey = (ItemKey) o;
        return Objects.equals(keyString, itemKey.keyString);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(keyString);
    }

    @Override
    public String toString() {
        return keyString;
    }
}
