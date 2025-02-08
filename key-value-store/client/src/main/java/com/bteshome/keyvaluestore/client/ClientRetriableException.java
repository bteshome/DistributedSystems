package com.bteshome.keyvaluestore.client;

public class ClientRetriableException extends RuntimeException {
    public ClientRetriableException(String message) {
        super(message);
    }
    public ClientRetriableException(Throwable cause) {
        super(cause);
    }
    public ClientRetriableException(String message, Throwable cause) {
        super(message, cause);
    }
}
