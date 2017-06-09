package nl.esciencecenter.xenon.grpc;

import java.io.File;
import java.io.IOException;

import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.ClientAuth;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import nl.esciencecenter.xenon.XenonFactory;
import nl.esciencecenter.xenon.grpc.files.FilesService;
import nl.esciencecenter.xenon.grpc.jobs.JobsService;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

public class XenonServerWrapper {
    private static final String PROGRAM_NAME = "xenon-grpc-server";
    private static final Logger LOGGER = LoggerFactory.getLogger(XenonServerWrapper.class);
    static final Integer DEFAULT_PORT = 50051;
    private final ArgumentParser parser = buildArgumentParser();
    private File serverPrivateKey = null;
    private File clientCertChain = null;
    private File serverCertChain = null;
    private Integer port = DEFAULT_PORT;

    private Server server;
    private boolean useTLS = false;


    public static void main(String[] args) throws InterruptedException, IOException {
        final XenonServerWrapper server;
        server = new XenonServerWrapper();
        server.start(args);
        server.blockUntilShutdown();
    }

    ArgumentParser buildArgumentParser() {
        ArgumentParser myparser = ArgumentParsers.newArgumentParser(PROGRAM_NAME)
                .defaultHelp(true)
                .description("gRPC (http://www.grpc.io/) server for Xenon (https://nlesc.github.io/Xenon/)");
        myparser.addArgument("--port", "-p")
                .type(Integer.class).setDefault(DEFAULT_PORT)
                .help("Port to bind to");
        ArgumentGroup serverGroup = myparser
                .addArgumentGroup("mutual TLS")
                .description("Encrypted, client and server authenticated connection, " +
                        "all arguments are required for an encrypted, authenticated connection, " +
                        "if none are supplied then an unencrypted, unauthenticated connection is used");
        serverGroup.addArgument("--server-cert-chain")
                .type(Arguments.fileType().verifyCanRead())
                .help("Certificate chain file in PEM format for server");
        serverGroup.addArgument("--server-private-key")
                .type(Arguments.fileType().verifyCanRead())
                .help("Private key file in PEM format for server");
        serverGroup.addArgument("--client-cert-chain")
                .type(Arguments.fileType().verifyCanRead())
                .help("Certificate chain file in PEM format for trusted client");
        return myparser;
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private void start(String[] args) throws IOException {
        try {
            parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            return;
        }

        serverBuilder();

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            LOGGER.info("*** shutting down gRPC server since JVM is shutting down");
            XenonServerWrapper.this.stop();
            LOGGER.info("*** server shut down");
        }));
    }

    private void serverBuilder() throws IOException {
        XenonSingleton singleton = new XenonSingleton();
        ServerBuilder<?> builder;
        if (useTLS) {
            builder = secureServerBuilder();
        } else {
            builder = insecureServerBuilder();
        }
        server = builder
                .addService(new GlobalService(singleton))
                .addService(new JobsService(singleton))
                .addService(new FilesService(singleton))
                .build();
    }

    void parseArgs(String[] args) throws ArgumentParserException {
        Namespace res = parser.parseArgs(args);
        port = res.getInt("port");
        serverCertChain = optionalFileArgument(res, "server_cert_chain");
        serverPrivateKey = optionalFileArgument(res, "server_private_key");
        clientCertChain = optionalFileArgument(res, "client_cert_chain");
        useTLS = (serverCertChain != null && serverPrivateKey != null && clientCertChain != null);
        boolean anyTLS = (serverCertChain != null || serverPrivateKey != null || clientCertChain != null);
        if (!useTLS && anyTLS) {
            throw new ArgumentParserException("Unable to enable mutual TLS. mutual TLS requires --server-cert-chain, --server-private-key and --client-cert-chain arguments set", parser);
        }
    }

    private ServerBuilder<?> secureServerBuilder() throws SSLException {
        LOGGER.info("Server started, listening on port {} with mutual TLS", port);
        LOGGER.info("On client use:");
        LOGGER.info("- {} as server certificate chain file", serverCertChain);
        LOGGER.info("- {} as client certificate chain file", clientCertChain);
        return NettyServerBuilder.forPort(port)
                .sslContext(GrpcSslContexts.forServer(serverCertChain, serverPrivateKey)
                        .trustManager(clientCertChain)
                        .clientAuth(ClientAuth.REQUIRE)
                        .build()
                );
    }


    private ServerBuilder<?> insecureServerBuilder() {
        LOGGER.info("Server started, listening on port {}", port);
        return ServerBuilder.forPort(port);
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
        XenonFactory.endAll();
    }

    private File optionalFileArgument(Namespace res, String key) {
        String value = res.getString(key);
        if (value != null) {
            return new File(value);
        }
        return null;
    }

    Integer getPort() {
        return port;
    }

    boolean getUseTLS() {
        return useTLS;
    }
}
