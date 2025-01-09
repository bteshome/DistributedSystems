package com.bteshome.keyvaluestore.common;

import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import lombok.Getter;

@Getter
public class ResponseStatus {
    private int httpStatusCode;

    public static int extractStatusCode(String messageString) {
        String[] messageParts = messageString.split(" ");
        return Integer.parseInt(messageParts[0]);
    }

    public static GenericResponse toGenericResponse(String messageString) {
        String[] messageParts = messageString.split(" ");
        return JavaSerDe.deserialize(messageParts[1]);
    }
}
