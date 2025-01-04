package com.bteshome.apigateway.ratelimiter;

import com.bteshome.apigateway.config.AppSettings;
import com.bteshome.consistenthashing.Ring;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Servers implements CommandLineRunner {
    @Autowired
    AppSettings appSettings;

    private static Ring ring = null;

    public String getServer(String client) {
        return ring.getServer(client);
    }

    @Override
    public void run(String... args) throws Exception {
        if (appSettings.isRateLimiterDisabled()) {
            log.info("Rate limiter is disabled. Skipping cache server ring construction.");
            return;
        }

        log.info("Rate limiter is enabled. Constructing cache server ring.");
        String[] servers = appSettings.getRateLimiterCacheServers().split(",");
        int numOfVirtualNodes = Integer.parseInt(appSettings.getRateLimiterNumOfVirtualNodes());
        ring = new Ring(numOfVirtualNodes);
        ring.addServers(servers);
        log.info("Cache server ring constructed.");
    }
}
