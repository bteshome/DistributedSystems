package com.bteshome.onlinestore.orderinguiconfig.controller;

import com.bteshome.onlinestore.orderinguiconfig.dto.ConfigGetRequest;
import com.bteshome.onlinestore.orderinguiconfig.dto.ConfigGetResponse;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.util.Strings;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class DefaultController {
    @PostMapping("/")
    public ResponseEntity<ConfigGetResponse> create(@RequestBody ConfigGetRequest request) {
        if (request == null || Strings.isBlank(request.getName())) {
            return ResponseEntity.ok(
                    ConfigGetResponse.builder()
                            .httpStatus(HttpStatus.BAD_REQUEST.value())
                            .errorMessage("Config name is null.")
                            .build());
        }

        try {
            log.debug("Reading config '%s'.".formatted(request.getName()));
            String value = Files.readString(Path.of(request.getName()));
            return ResponseEntity.ok(
                    ConfigGetResponse.builder()
                            .httpStatus(HttpStatus.OK.value())
                            .value(value)
                            .build());
        } catch (IOException e) {
            String errorMessage = "Error reading config '%s'.".formatted(request.getName());
            log.error(errorMessage, e);
            return ResponseEntity.ok(
                    ConfigGetResponse.builder()
                            .httpStatus(HttpStatus.INTERNAL_SERVER_ERROR.value())
                            .errorMessage(errorMessage)
                            .build());
        }
    }
}