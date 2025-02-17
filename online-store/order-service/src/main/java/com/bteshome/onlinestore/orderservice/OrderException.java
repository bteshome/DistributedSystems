package com.bteshome.onlinestore.orderservice;

public class OrderException extends RuntimeException {
    public OrderException(String message) {
        super(message);
    }
    public OrderException(Throwable cause) {
        super(cause);
    }
    public OrderException(String message, Throwable cause) {
        super(message, cause);
    }
}
