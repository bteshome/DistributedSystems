package com.bteshome.ratelimiterrulesdashboard.controller;

import com.bteshome.ratelimiterrulesdashboard.dto.RuleRequest;
import com.bteshome.ratelimiterrulesdashboard.service.RuleService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.*;

@Controller
@Slf4j
public class RuleController {
    @Autowired
    private RuleService ruleService;

    @GetMapping("/")
    public String get(Model model) {
        var rules = ruleService.getAll();
        model.addAttribute("rules", rules);
        return "index.html";
    }

    @GetMapping("/create")
    public String create(Model model) {
        RuleRequest rule = RuleRequest.builder()
                .isPerClient(true)
                .granularity("MINUTE")
                .threshold(5)
                .build();
        model.addAttribute("rule", rule);
        return "create.html";
    }

    @PostMapping("/create")
    public String create(@ModelAttribute("rule") @RequestBody RuleRequest rule, Errors errors) {
        if (errors.hasErrors()) {
            log.error("Rule validation errors: " + errors.toString());
            return "create.html";
        }

        ruleService.create(rule);
        return "redirect:/";
    }
}
