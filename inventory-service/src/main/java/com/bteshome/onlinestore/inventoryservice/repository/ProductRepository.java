package com.bteshome.onlinestore.inventoryservice.repository;

import com.bteshome.keyvaluestore.client.clientrequests.ItemGet;
import com.bteshome.keyvaluestore.client.clientrequests.ItemList;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.client.readers.BatchReader;
import com.bteshome.keyvaluestore.client.readers.ItemReader;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.requests.IsolationLevel;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.client.writers.ItemWriter;
import com.bteshome.onlinestore.inventoryservice.InventoryException;
import com.bteshome.onlinestore.inventoryservice.model.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.stream.Stream;

@Repository
public class ProductRepository {
    private final String tableName = "products";
    @Autowired
    private ItemWriter itemWriter;
    @Autowired
    private ItemReader itemReader;
    @Autowired
    private BatchReader batchReader;

    public void put(Product product) {
        ItemWrite<Product> request = new ItemWrite<>();
        request.setTable(tableName);
        request.setKey(product.getSkuCode());
        request.setAck(AckType.MIN_ISR_COUNT);
        request.setMaxRetries(0);
        request.setValue(product);

        ItemPutResponse itemPutResponse = itemWriter.putObject(request).block();

        if (itemPutResponse.getHttpStatusCode() != HttpStatus.OK.value()) {
            String errorMessage = "Failed to create product %s. Status code=%s, error message=%s".formatted(
                    product.getSkuCode(),
                    itemPutResponse.getHttpStatusCode(),
                    itemPutResponse.getErrorMessage());
            throw new InventoryException(errorMessage);
        }
    }

    public Product get(String skuCode) {
        ItemGet request = new ItemGet();
        request.setTable(tableName);
        request.setKey(skuCode);
        request.setIsolationLevel(IsolationLevel.READ_COMMITTED);

        return itemReader.getObject(request, Product.class).block();
    }

    public Stream<Product> getAll() {
        ItemList listRequest = new ItemList();
        listRequest.setTable(tableName);
        listRequest.setLimit(10);
        listRequest.setIsolationLevel(IsolationLevel.READ_COMMITTED);

        return batchReader
                .listObjects(listRequest, Product.class)
                .collectList()
                .block()
                .stream()
                .map(Map.Entry::getValue);
     }
}
