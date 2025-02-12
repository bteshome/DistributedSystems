package com.bteshome.keyvaluestore.storage.entities;

import com.bteshome.keyvaluestore.common.LogPosition;

import java.io.Serializable;

public record ItemValueVersion(LogPosition offset, byte[] bytes, long expiryTime) implements Serializable {
    public static ItemValueVersion of(LogPosition offset, byte[] bytes, long expiryTime) {
        return new ItemValueVersion(offset, bytes, expiryTime);
    }
}
