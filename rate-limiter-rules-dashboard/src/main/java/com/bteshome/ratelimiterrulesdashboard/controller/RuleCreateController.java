package com.bteshome.ratelimiterrulesdashboard.controller;

import com.bteshome.ratelimiterrulesdashboard.dto.RuleRequest;
import com.bteshome.ratelimiterrulesdashboard.service.RuleService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/create")
@Slf4j
public class RuleCreateController {
    @Autowired
    private RuleService ruleService;

    @GetMapping("/")
    public String create(Model model) {
        RuleRequest rule = RuleRequest.builder()
                .isPerClient(true)
                .granularity("MINUTE")
                .threshold(5)
                .build();
        model.addAttribute("rule", rule);
        model.addAttribute("page", "create");
        return "create.html";
    }

    @PostMapping("/")
    public String create(@ModelAttribute("rule") @RequestBody RuleRequest rule, Model model) {
        try {
            ruleService.create(rule);
            model.addAttribute("info", "CREATE succeeded.");
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("rule", rule);
        model.addAttribute("page", "create");
        return "create.html";
    }
}
