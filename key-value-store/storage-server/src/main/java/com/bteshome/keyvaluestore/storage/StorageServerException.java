package com.bteshome.keyvaluestore.storage;

public class StorageServerException extends RuntimeException {
    public StorageServerException(String message) {
        super(message);
    }
    public StorageServerException(Throwable cause) {
        super(cause);
    }
    public StorageServerException(String message, Throwable cause) {
        super(message, cause);
    }
}
