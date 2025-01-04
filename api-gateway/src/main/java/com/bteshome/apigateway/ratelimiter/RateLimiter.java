package com.bteshome.apigateway.ratelimiter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.UnifiedJedis;
import java.time.LocalDateTime;

@Component
public class RateLimiter {
    @Autowired
    Servers servers;

    public synchronized boolean tryAcquire(String api, String client) {
        var rules = RuleCache.getRules(api);

        if (rules == null) {
            return true;
        }

        String server = servers.getServer(client);

        try (var redisClient = new UnifiedJedis(server)) {
            for (var rule : rules) {
                boolean allowed = switch (rule.getGranularity()) {
                    case MINUTE -> tryAcquirePerMinute(rule, redisClient, client);
                    case SECOND -> tryAcquirePerSecond(rule, redisClient, client);
                    case null, default -> throw new RateLimiterException("Invalid rule granularity: " + rule.getGranularity());
                };
                if (!allowed) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean tryAcquirePerMinute(Rule rule, UnifiedJedis redisClient, String client) {
        int nowSecond = LocalDateTime.now().getSecond();
        int now = LocalDateTime.now().getMinute();

        String key = toKeyString(rule, client);
        String nowKey = key + ":" + now;
        String previousKey = key + ":" + (now == 0 ? 59 : (now - 1));

        if (redisClient.get(nowKey) == null) {
            redisClient.set(nowKey, "1");
            redisClient.expire(nowKey, 120 - nowSecond);
            return true;
        }

        long count = redisClient.incr(nowKey);
        String previousCount = redisClient.get(previousKey);

        if (previousCount != null) {
            count += ((60 - nowSecond) / 60) * Long.parseLong(previousCount);
        }

        return count <= rule.getThreshold();
    }

    private boolean tryAcquirePerSecond(Rule rule, UnifiedJedis redisClient, String client) {
        int now = LocalDateTime.now().getSecond();

        String key = toKeyString(rule, client);
        String nowKey = key + ":" + now;

        if (redisClient.get(nowKey) == null) {
            redisClient.set(nowKey, "1");
            redisClient.expire(nowKey, 2);
            return true;
        }

        long count = redisClient.incr(nowKey);

        return count <= rule.getThreshold();
    }

    private String toKeyString(Rule rule, String client) {
        return "%s:%s:%s".formatted(rule.getApi(), rule.isPerClient() ? client : "*", rule.getGranularity()).toLowerCase();
    }
}