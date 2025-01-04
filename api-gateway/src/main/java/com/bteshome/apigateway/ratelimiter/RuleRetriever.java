package com.bteshome.apigateway.ratelimiter;

import com.bteshome.apigateway.config.AppSettings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Component
@Slf4j
public class RuleRetriever {
    @Autowired
    AppSettings appSettings;

    public void fetchRules() {
        log.debug("Attempting to fetch rate limiter rules...");

        final HashMap<String, List<Rule>> rules = new HashMap<>();

        try (var c = DriverManager.getConnection(appSettings.getRateLimiterRulesDbConnectionString())) {
            var s = c.prepareStatement("select api, is_per_client, granularity, threshold from rules");
            var rs = s.executeQuery();

            while (rs.next()) {
                String api = rs.getString(1);
                boolean isPerClient = rs.getBoolean(2);
                Granularity granularity = Granularity.valueOf(rs.getString(3));
                int threshold = rs.getInt(4);

                if (!rules.containsKey(api)) {
                    rules.put(api, new ArrayList<>());
                }

                rules.get(api).add(
                    Rule.builder(api, granularity, threshold)
                        .isPerClient(isPerClient)
                        .build());
            }

            RuleCache.setRules(rules);

            log.debug("Successfully fetched rate limiter rules: {}", rules);
        } catch (Exception e) {
            log.error("Failed to fetch rate limiter rules", e);
        }
    }
}