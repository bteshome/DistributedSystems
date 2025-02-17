package com.bteshome.ratelimiterrulesdashboard.service;

import com.bteshome.ratelimiterrulesdashboard.Granularity;
import com.bteshome.ratelimiterrulesdashboard.common.RateLimiterRuleException;
import com.bteshome.ratelimiterrulesdashboard.dto.RuleRequest;
import com.bteshome.ratelimiterrulesdashboard.dto.RuleResponse;
import com.bteshome.ratelimiterrulesdashboard.model.Rule;
import com.bteshome.ratelimiterrulesdashboard.repository.RuleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
public class RuleService {
    @Autowired
    private RuleRepository ruleRepository;

    public void create(RuleRequest ruleRequest) {
        try {
            log.info("Creating rule: {}", ruleRequest);

            Rule rule = mapToRule(ruleRequest);
            rule.setId(UUID.randomUUID());
            ruleRepository.create(rule);

            log.info("Rule created. Id: {}", rule.getId());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RateLimiterRuleException(e.getMessage(), e);
        }
    }

    public Stream<RuleResponse> getAll() {
        try {
            return ruleRepository
                    .getAll()
                    .map(this::mapToRuleResponse);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RateLimiterRuleException(e.getMessage(), e);
        }
    }

    private Rule mapToRule(RuleRequest ruleRequest) {
        return Rule.builder(
                        ruleRequest.getApi(),
                        Granularity.valueOf(ruleRequest.getGranularity()),
                        ruleRequest.getThreshold())
                .isPerClient(ruleRequest.isPerClient())
                .build();
    }

    private RuleResponse mapToRuleResponse(Rule rule) {
        return RuleResponse.builder()
                .id(rule.getId())
                .api(rule.getApi())
                .granularity(rule.getGranularity())
                .threshold(rule.getThreshold())
                .isPerClient(rule.isPerClient())
                .build();
    }
}