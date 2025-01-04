package com.bteshome.apigateway.ratelimiter;

import java.util.HashMap;
import java.util.List;

class RuleCache {
    private static HashMap<String, List<Rule>> rules = new HashMap<>();

    static List<Rule> getRules(String api) {
        return rules.getOrDefault(api, null);
    }

    synchronized static void setRules(HashMap<String, List<Rule>> rules) {
        RuleCache.rules = rules;
    }
}
