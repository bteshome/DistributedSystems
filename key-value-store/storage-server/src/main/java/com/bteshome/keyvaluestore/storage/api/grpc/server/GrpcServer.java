package com.bteshome.keyvaluestore.storage.api.grpc.server;

import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import lombok.extern.slf4j.Slf4j;
import io.grpc.*;

import java.util.List;

@Slf4j
public class GrpcServer implements AutoCloseable {
    private Server server;

    private GrpcServer() {}

    public static GrpcServer create(int port, BindableService... services) {
        ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port);
        for (BindableService service : services)
            serverBuilder.addService(service);
        Server server = serverBuilder.build();
        GrpcServer grpcServer = new GrpcServer();
        grpcServer.server = server;
        return grpcServer;
    }

    public void start() {
        try {
            List<String> serviceNames = server.getServices()
                    .stream()
                    .map(ServerServiceDefinition::getServiceDescriptor)
                    .map(ServiceDescriptor::getName)
                    .toList();
            log.info("Starting gRPC server. Services: {}.", serviceNames);
            server.start();
            log.info("gRPC server started successfully. Listening on port: {}.", this.server.getPort());
            server.awaitTermination();
        } catch (Exception e) {
            log.error("Error starting gRPC server.", e);
            throw new StorageServerException(e);
        }
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdown();
            log.info("gRPC server shut down successfully.");
        }
    }
}
