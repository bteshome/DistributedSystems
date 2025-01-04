package com.bteshome.onlinestore.orderservice;

public class InventoryClientException extends RuntimeException {
    public InventoryClientException(String message) {
        super(message);
    }
    public InventoryClientException(Throwable cause) {
        super(cause);
    }
    public InventoryClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
