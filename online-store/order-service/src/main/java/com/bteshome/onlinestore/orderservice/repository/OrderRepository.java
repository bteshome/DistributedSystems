package com.bteshome.onlinestore.orderservice.repository;

import com.bteshome.keyvaluestore.client.clientrequests.ItemGet;
import com.bteshome.keyvaluestore.client.clientrequests.ItemList;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.client.readers.BatchReader;
import com.bteshome.keyvaluestore.client.readers.ItemReader;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.requests.IsolationLevel;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.client.writers.ItemWriter;
import com.bteshome.onlinestore.orderservice.OrderException;
import com.bteshome.onlinestore.orderservice.model.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.stream.Stream;

@Repository
public class OrderRepository {
    private final String tableName = "orders";
    @Autowired
    private ItemWriter itemWriter;
    @Autowired
    private ItemReader itemReader;
    @Autowired
    private BatchReader batchReader;

    public void put(Order order) {
        ItemWrite<Order> request = new ItemWrite<>();
        request.setTable(tableName);
        request.setKey(order.getOrderNumber());
        request.setAck(AckType.MIN_ISR_COUNT);
        request.setMaxRetries(0);
        request.setValue(order);

        ItemPutResponse itemPutResponse = itemWriter.putObject(request).block();

        if (itemPutResponse.getHttpStatusCode() != HttpStatus.OK.value()) {
            String errorMessage = "Failed to place order %s. Status code=%s, error message=%s".formatted(
                    order.getOrderNumber(),
                    itemPutResponse.getHttpStatusCode(),
                    itemPutResponse.getErrorMessage());
            throw new OrderException(errorMessage);
        }
    }

    public Order get(String orderNumber) {
        ItemGet request = new ItemGet();
        request.setTable(tableName);
        request.setKey(orderNumber);
        request.setIsolationLevel(IsolationLevel.READ_COMMITTED);

        return itemReader.getObject(request, Order.class).block();
    }

    public Stream<Order> getAll() {
        ItemList listRequest = new ItemList();
        listRequest.setTable(tableName);
        listRequest.setLimit(10);
        listRequest.setIsolationLevel(IsolationLevel.READ_COMMITTED);

        return batchReader
                .listObjects(listRequest, Order.class)
                .collectList()
                .block()
                .stream()
                .map(Map.Entry::getValue);
    }
}
