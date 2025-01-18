package com.bteshome.keyvaluestore.admindashboard.common;

public class AdminDashboardException extends RuntimeException {
    public AdminDashboardException(String message) {
        super(message);
    }
    public AdminDashboardException(Throwable cause) {
        super(cause);
    }
    public AdminDashboardException(String message, Throwable cause) {
        super(message, cause);
    }
}
