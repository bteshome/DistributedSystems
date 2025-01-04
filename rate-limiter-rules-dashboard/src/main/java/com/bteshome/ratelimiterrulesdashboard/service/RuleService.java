package com.bteshome.ratelimiterrulesdashboard.service;

import com.bteshome.ratelimiterrulesdashboard.Granularity;
import com.bteshome.ratelimiterrulesdashboard.dto.RuleRequest;
import com.bteshome.ratelimiterrulesdashboard.dto.RuleResponse;
import com.bteshome.ratelimiterrulesdashboard.model.Rule;
import com.bteshome.ratelimiterrulesdashboard.repository.RuleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.StreamSupport;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class RuleService {
    @Autowired
    private final RuleRepository ruleRepository;

    public void create(RuleRequest ruleRequest) {
        log.info("Creating rule: {}", ruleRequest);

        Rule rule = mapToRule(ruleRequest);
        rule = ruleRepository.save(rule);

        log.info("Rule created. Id: {}", rule.getId());
    }

    public List<RuleResponse> getAll() {
        Iterable<Rule> products = ruleRepository.findAll();
        return StreamSupport.stream(products.spliterator(), false)
                .map(this::mapToRuleResponse)
                .toList();
    }

    private Rule mapToRule(RuleRequest ruleRequest) {
        return Rule.builder(
                        ruleRequest.getApi(),
                        Granularity.valueOf(ruleRequest.getGranularity()),
                        ruleRequest.getThreshold())
                .isPerClient(ruleRequest.isPerClient())
                .build();
    }

    private RuleResponse mapToRuleResponse(Rule product) {
        return RuleResponse.builder()
                .api(product.getApi())
                .granularity(product.getGranularity())
                .threshold(product.getThreshold())
                .isPerClient(product.isPerClient())
                .build();
    }
}