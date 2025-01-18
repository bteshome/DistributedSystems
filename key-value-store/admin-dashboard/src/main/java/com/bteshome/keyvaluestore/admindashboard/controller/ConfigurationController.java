package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.common.ConfigurationCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Map;

@Controller
@RequestMapping("/configurations")
@RequiredArgsConstructor
@Slf4j
public class ConfigurationController {
    @Autowired
    ConfigurationCache configurationCache;

    @GetMapping("/")
    public String list(Model model) {
        Map<String, Object> configurations = configurationCache.getConfigurations();
        model.addAttribute("configurations", configurations.entrySet().stream().toList());
        model.addAttribute("page", "configurations");
        return "configurations.html";
    }
}