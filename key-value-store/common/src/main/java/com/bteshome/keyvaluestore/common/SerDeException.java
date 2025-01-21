package com.bteshome.keyvaluestore.common;

public class SerDeException extends RuntimeException {
    public SerDeException(String message) {
        super(message);
    }
    public SerDeException(Throwable cause) {
        super(cause);
    }
    public SerDeException(String message, Throwable cause) {
        super(message, cause);
    }
}
