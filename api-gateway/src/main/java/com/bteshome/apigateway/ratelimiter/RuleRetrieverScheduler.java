package com.bteshome.apigateway.ratelimiter;

import com.bteshome.apigateway.common.AppSettings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class RuleRetrieverScheduler implements CommandLineRunner {
    private ScheduledExecutorService executor = null;

    @Autowired
    AppSettings appSettings;

    @Autowired
    RuleRetriever ruleRetriever;

    @Override
    public void run(String... args) throws Exception {
        if (appSettings.isRateLimiterDisabled()) {
            log.info("Rate limiter is disabled. Skipping rule retriever scheduling.");
            return;
        }

        log.info("Rate limiter is enabled. Scheduling rule retriever ...");

        final int syncFrequency = Integer.parseInt(appSettings.getRateLimiterRulesSyncFrequencySeconds());
        executor = Executors.newSingleThreadScheduledExecutor();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                executor.close();
            }
        });
        executor.scheduleAtFixedRate(() -> ruleRetriever.fetchRules(), 1L, syncFrequency, TimeUnit.SECONDS);

        log.info("Scheduled rule retriever with frequency {} seconds", syncFrequency);
    }
}