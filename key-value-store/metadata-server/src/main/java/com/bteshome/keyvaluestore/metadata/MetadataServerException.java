package com.bteshome.keyvaluestore.metadata;

public class MetadataServerException extends RuntimeException {
    public MetadataServerException(String message) {
        super(message);
    }
    public MetadataServerException(Throwable cause) {
        super(cause);
    }
    public MetadataServerException(String message, Throwable cause) {
        super(message, cause);
    }
}
