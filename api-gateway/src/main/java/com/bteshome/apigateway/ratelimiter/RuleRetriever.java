package com.bteshome.apigateway.ratelimiter;

import com.bteshome.keyvaluestore.client.clientrequests.ItemList;
import com.bteshome.keyvaluestore.client.readers.ItemLister;
import com.bteshome.keyvaluestore.client.requests.IsolationLevel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class RuleRetriever {
    private final String tableName = "rate_limiter_rules";
    @Autowired
    ItemLister itemLister;

    public void fetchRules() {
        log.debug("Attempting to fetch rate limiter rules...");

        final HashMap<String, List<Rule>> rules = new HashMap<>();

        try {
            ItemList listRequest = new ItemList();
            listRequest.setTable(tableName);
            listRequest.setLimit(10);
            listRequest.setIsolationLevel(IsolationLevel.READ_COMMITTED);

            itemLister
                    .listObjects(listRequest, Rule.class)
                    .collectList()
                    .block()
                    .stream()
                    .map(Map.Entry::getValue).forEach(rule -> {
                        String api = rule.getApi();

                        if (!rules.containsKey(api))
                            rules.put(api, new ArrayList<>());

                        rules.get(api).add(rule);
                    });

            RuleCache.setRules(rules);

            log.debug("Successfully fetched rate limiter rules: {}", rules);
        } catch (Exception e) {
            log.error("Failed to fetch rate limiter rules", e);
        }
    }
}