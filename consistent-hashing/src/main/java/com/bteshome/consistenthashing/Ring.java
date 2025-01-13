package com.bteshome.consistenthashing;

import org.apache.commons.codec.digest.DigestUtils;

import java.math.BigInteger;
import java.util.Collection;
import java.util.TreeMap;

public class Ring {
    private final int numOfVirtualNodes;
    private final TreeMap<BigInteger, String> ring;

    public Ring() {
        this(3);
    }

    public Ring(int numOfVirtualNodes) {
        ring = new TreeMap<>();
        this.numOfVirtualNodes = numOfVirtualNodes;
    }

    public void addServers(Collection<String> servers) {
        for (String server : servers) {
            addServer(server);
        }
    }

    public void addServers(String[] servers) {
        for (String server : servers) {
            addServer(server);
        }
    }

    public void addServer(String server) {
        for (int i = 0; i < numOfVirtualNodes; i++) {
            var hash = getHash(server + "_" + i);
            ring.put(hash, server);
        }
    }

    public String getServer(String key) {
        if (ring.isEmpty()) {
            return null;
        }

        var hash = getHash(key);

        if (!ring.containsKey(hash)) {
            var tailMap = ring.tailMap(hash);
            if (tailMap.isEmpty()) {
                hash = ring.firstKey();
            } else {
                hash = tailMap.firstKey();
            }
        }

        return ring.get(hash);
    }

    private BigInteger getHash(String key) {
        var hex = DigestUtils.md5Hex(key);
        return new BigInteger(hex, 16);
    }

    public void removeServer(String server) {
        for (int i = 0; i < numOfVirtualNodes; i++) {
            var hash = getHash(server + "_" + i);
            ring.remove(hash);
        }
    }

    public void showRing() {
        for (var key : ring.keySet()) {
            System.out.println(key + ": " + ring.get(key));
        }
    }
}