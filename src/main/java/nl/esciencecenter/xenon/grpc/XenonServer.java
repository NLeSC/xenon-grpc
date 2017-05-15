package nl.esciencecenter.xenon.grpc;

import java.io.IOException;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XenonServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(XenonServer.class);

    private Server server;

    public static void main(String[] args) throws InterruptedException, IOException {
        final XenonServer server = new XenonServer();
        server.start();
        server.blockUntilShutdown();
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private void start() throws IOException {
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new XenonJobsImpl())
                .addService(new XenonFilesImpl())
                .build()
                .start();
        LOGGER.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            XenonServer.this.stop();
            System.err.println("*** server shut down");
        }));
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
}
