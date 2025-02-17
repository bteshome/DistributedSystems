package com.bteshome.ratelimiterrulesdashboard.common;

public class RateLimiterRuleException extends RuntimeException {
    public RateLimiterRuleException(String message) {
        super(message);
    }
    public RateLimiterRuleException(Throwable cause) {
        super(cause);
    }
    public RateLimiterRuleException(String message, Throwable cause) {
        super(message, cause);
    }
}
