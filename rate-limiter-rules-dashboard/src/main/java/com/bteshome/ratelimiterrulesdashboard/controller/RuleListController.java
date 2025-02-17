package com.bteshome.ratelimiterrulesdashboard.controller;

import com.bteshome.ratelimiterrulesdashboard.dto.RuleResponse;
import com.bteshome.ratelimiterrulesdashboard.service.RuleService;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;

@Controller
@RequestMapping("/")
@RequiredArgsConstructor
@Slf4j
public class RuleListController {
    @Autowired
    private RuleService ruleService;

    @GetMapping
    public String getAll(Model model) {
        try {
            var rules = ruleService.getAll().toList();
            model.addAttribute("rules", rules);
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("page", "list");
        return "list.html";
    }
}
