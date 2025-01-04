package com.bteshome.onlinestore.inventoryservice;

public class InventoryException extends RuntimeException {
    public InventoryException(String message) {
        super(message);
    }
    public InventoryException(Throwable cause) {
        super(cause);
    }
    public InventoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
