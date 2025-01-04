package com.bteshome.keyvaluestore.storage;

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyFactory;
import org.apache.ratis.netty.client.NettyClientRpc;
import org.apache.ratis.protocol.*;
import org.apache.ratis.util.JavaUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class Node implements CommandLineRunner {
    @Autowired
    AppSettings appSettings;

    @Autowired
    Utils utils;

    @Override
    public void run(String... args){
        try (RaftClient client = utils.createRaftClient()) {
            JoinNodeMessage message = new JoinNodeMessage(
                    appSettings.getNodeId(),
                    appSettings.getHost(),
                    appSettings.getPort(),
                    appSettings.getJmxPort(),
                    appSettings.getRack()
            );
            log.info("Joining cluster: {}", message);
            /*final RaftClientReply reply = client.io().send(message);
            if (reply.isSuccess()) {
                log.info("Joined cluster");
            } else {
                log.error("Error joining cluster: ", reply.getException());
                throw new StorageServerException(reply.getException());
            }*/
        } catch (Exception e) {
            log.error("Error occurred: ", e);
            throw new StorageServerException(e);
        }
    }
}
