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

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@Order(-1)
public class ClientMetadataRefresher implements GlobalFilter {
    private final AtomicLong lastRefreshTime = new AtomicLong(0);
    @Autowired
    private ClientMetadataFetcher clientMetadataFetcher;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        long now = System.currentTimeMillis();
        long lastTime = lastRefreshTime.get();

        if (now - lastTime > 10000 && lastRefreshTime.compareAndSet(lastTime, now))
            clientMetadataFetcher.fetch();

        return chain.filter(exchange);
    }
}