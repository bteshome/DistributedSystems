package com.bteshome.keyvaluestore.adminclient.common;

public class AdminClientException extends RuntimeException {
    public AdminClientException(String message) {
        super(message);
    }
    public AdminClientException(Throwable cause) {
        super(cause);
    }
    public AdminClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
