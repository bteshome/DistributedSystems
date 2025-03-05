package com.bteshome.ratelimiterrulesdashboard.repository;

import com.bteshome.keyvaluestore.client.clientrequests.ItemList;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.client.readers.ItemLister;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.requests.IsolationLevel;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.client.writers.ItemWriter;
import com.bteshome.ratelimiterrulesdashboard.common.RateLimiterRuleException;
import com.bteshome.ratelimiterrulesdashboard.model.Rule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.stream.Stream;

@Repository
public class RuleRepository {
    @Value("${table-name}")
    private String tableName;
    @Autowired
    private ItemWriter itemWriter;
    @Autowired
    private ItemLister itemLister;

    public void create(Rule rule) {
        ItemWrite<Rule> request = new ItemWrite<>();
        request.setTable(tableName);
        request.setKey(rule.getId().toString());
        request.setAck(AckType.MIN_ISR_COUNT);
        request.setMaxRetries(0);
        request.setValue(rule);

        ItemPutResponse itemPutResponse = itemWriter.putObject(request).block();

        if (itemPutResponse.getHttpStatusCode() != HttpStatus.OK.value()) {
            String errorMessage = "Failed to create rule api=%s, granularity=%s, threshold=%s. Status code=%s, error message=%s".formatted(
                    rule.getApi(),
                    rule.getGranularity(),
                    rule.getThreshold(),
                    itemPutResponse.getHttpStatusCode(),
                    itemPutResponse.getErrorMessage());
            throw new RateLimiterRuleException(errorMessage);
        }
    }

    public Stream<Rule> getAll() {
        ItemList listRequest = new ItemList();
        listRequest.setTable(tableName);
        listRequest.setLimit(10);
        listRequest.setIsolationLevel(IsolationLevel.READ_COMMITTED);

        return itemLister
                .listObjects(listRequest, Rule.class)
                .collectList()
                .block()
                .stream()
                .map(Map.Entry::getValue);
    }
}