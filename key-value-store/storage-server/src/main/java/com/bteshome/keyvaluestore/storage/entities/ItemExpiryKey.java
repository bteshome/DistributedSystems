package com.bteshome.keyvaluestore.storage.entities;

import com.bteshome.keyvaluestore.common.LogPosition;

import java.io.Serializable;
import java.util.Objects;

public record ItemExpiryKey(String keyString, LogPosition offset, long expiryTime) implements Serializable {
    public static ItemExpiryKey of(String keyString, LogPosition offset, long expiryTime) {
        return new ItemExpiryKey(keyString, offset, expiryTime);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ItemExpiryKey that = (ItemExpiryKey) o;
        return Objects.equals(keyString, that.keyString) && Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyString, offset);
    }

    @Override
    public String toString() {
        return keyString + "@" + offset.toString();
    }
}
