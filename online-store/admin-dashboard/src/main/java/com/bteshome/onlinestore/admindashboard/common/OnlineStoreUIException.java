package com.bteshome.onlinestore.admindashboard.common;

public class OnlineStoreUIException extends RuntimeException {
    public OnlineStoreUIException(String message) {
        super(message);
    }
    public OnlineStoreUIException(Throwable cause) {
        super(cause);
    }
    public OnlineStoreUIException(String message, Throwable cause) {
        super(message, cause);
    }
}
