package com.bteshome.apigateway.ratelimiter;

public class RateLimiterException extends RuntimeException {
    public RateLimiterException(String message) {
        super(message);
    }
    public RateLimiterException(Throwable cause) {
        super(cause);
    }
    public RateLimiterException(String message, Throwable cause) {
        super(message, cause);
    }
}
