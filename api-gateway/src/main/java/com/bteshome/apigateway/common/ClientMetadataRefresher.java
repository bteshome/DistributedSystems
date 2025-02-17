package com.bteshome.apigateway.common;

import com.bteshome.keyvaluestore.client.ClientMetadataFetcher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@Order(-1)
public class ClientMetadataRefresher implements GlobalFilter {
    private long lastRefreshTime = 0;
    @Autowired
    private ClientMetadataFetcher clientMetadataFetcher;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        if (System.currentTimeMillis() - lastRefreshTime > 10000) {
            clientMetadataFetcher.fetch();
            lastRefreshTime = System.currentTimeMillis();
        }
        return chain.filter(exchange);
    }
}