package com.bteshome.keyvaluestore.admindashboard.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ui.Model;

// TODO - there is an issue with the template.
//@ControllerAdvice(annotations = Controller.class)
@Slf4j
public class GlobalExceptionHandler {
    //@ExceptionHandler(produces = "text/html")
    public String handleException(Exception throwable, Model model) {
        log.error("An unhandled exception", throwable);
        model.addAttribute("errorMessage", throwable.getMessage());
        model.addAttribute("exception", throwable);
        return "error";
    }
}