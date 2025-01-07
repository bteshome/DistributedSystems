package com.bteshome.keyvaluestore.common;

import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import lombok.Getter;

@Getter
public class ResponseStatus {
    private int httpStatusCode;

    public static final int OK = 200;
    public static final int BAD_REQUEST = 400;
    public static final int NOT_FOUND = 404;
    public static final int CONFLICT = 409;
    public static final int INTERNAL_SERVER_ERROR = 500;

    public static int extractStatusCode(String messageString) {
        String[] messageParts = messageString.split(" ");
        return Integer.parseInt(messageParts[0]);
    }

    public static GenericResponse toGenericResponse(String messageString) {
        String[] messageParts = messageString.split(" ");
        return JavaSerDe.deserialize(messageParts[1]);
    }
}
