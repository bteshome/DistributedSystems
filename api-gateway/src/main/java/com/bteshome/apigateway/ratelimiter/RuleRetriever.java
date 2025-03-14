package com.bteshome.apigateway.ratelimiter;

import com.bteshome.apigateway.common.AppSettings;
import com.bteshome.keyvaluestore.client.ClientMetadataFetcher;
import com.bteshome.keyvaluestore.client.clientrequests.ItemList;
import com.bteshome.keyvaluestore.client.readers.ItemLister;
import com.bteshome.keyvaluestore.client.requests.IsolationLevel;
import com.bteshome.keyvaluestore.client.responses.CursorPosition;
import com.bteshome.keyvaluestore.client.responses.ItemListResponseFlattened;
import com.bteshome.keyvaluestore.client.responses.ItemResponse;
import com.bteshome.keyvaluestore.common.Tuple3;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Slf4j
public class RuleRetriever {
    @Autowired
    private AppSettings appSettings;
    @Autowired
    private ItemLister itemLister;
    @Autowired
    private ClientMetadataFetcher clientMetadataFetcher;

    public void fetchRules() {
        final HashMap<String, List<Rule>> rules = new HashMap<>();

        try {
            clientMetadataFetcher.fetch();

            log.debug("Attempting to fetch rate limiter rules...");

            ItemList listRequest = new ItemList();
            listRequest.setTable(appSettings.getRateLimiterRulesTableName());
            listRequest.setLimit(10);
            listRequest.setIsolationLevel(IsolationLevel.READ_COMMITTED);

            boolean hasMore = true;
            Map<Integer, CursorPosition> cursorPositions = new HashMap<>();

            while (hasMore) {
                listRequest.setCursorPositions(cursorPositions);
                ItemListResponseFlattened<Rule> response = itemLister
                        .listObjects(listRequest, Rule.class)
                        .block();

                if (response != null) {
                    response.getItems().forEach(item -> {
                        Rule rule = item.getValue();
                        String api = rule.getApi();

                        if (!rules.containsKey(api))
                            rules.put(api, new ArrayList<>());

                        rules.get(api).add(rule);
                    });
                    cursorPositions = response.getCursorPositions();
                    hasMore = response.hasMore();
                } else {
                    break;
                }
            }

            RuleCache.setRules(rules);

            log.debug("Successfully fetched rate limiter rules: {}", rules);
        } catch (Exception e) {
            log.error("Failed to fetch rate limiter rules", e);
        }
    }
}