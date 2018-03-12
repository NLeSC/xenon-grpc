package nl.esciencecenter.xenon.grpc;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLException;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.netty.handler.ssl.ClientAuth;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.grpc.filesystems.FileSystemService;
import nl.esciencecenter.xenon.grpc.schedulers.SchedulerService;

public class XenonServerWrapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(XenonServerWrapper.class);
    static final Integer DEFAULT_PORT = 50051;
    private final ArgumentParser parser = buildArgumentParser();
    private File serverPrivateKey = null;
    private File clientCertChain = null;
    private File serverCertChain = null;
    private Integer port = DEFAULT_PORT;
    private boolean useTLS = false;

    private Server server;
    private FileSystemService filesystemService;
    private SchedulerService schedulerService;

    public static void main(String[] args) throws InterruptedException, IOException {
        final XenonServerWrapper server;
        server = new XenonServerWrapper();
        server.start(args);
        server.blockUntilShutdown();
    }

    ArgumentParser buildArgumentParser() {
        ArgumentParser myparser = ArgumentParsers.newFor(BuildConfig.NAME).build()
                .defaultHelp(true)
                .description("gRPC (http://www.grpc.io/) server for Xenon (https://nlesc.github.io/Xenon/)")
                .version("Xenon gRPC v" + BuildConfig.VERSION + ", Xenon Library v" + BuildConfig.XENON_LIB_VERSION);
        myparser.addArgument("--version").action(Arguments.version()).help("Prints version and exists");
        myparser.addArgument("--verbose", "-v").help("Repeat for more verbose logging").action(Arguments.count());
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
        myparser.addArgument("--proto").action(storeTrue()).help("Print proto file of server and exits");
        return myparser;
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    void start(String[] args) throws IOException {
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
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            XenonServerWrapper.this.stop();
            System.err.println("*** server shut down");
        }));
    }

    private void serverBuilder() throws IOException {
        ServerBuilder<?> builder;
        if (useTLS) {
            builder = secureServerBuilder();
        } else {
            builder = insecureServerBuilder();
        }
        Map<String, FileSystem> fileSystems = new ConcurrentHashMap<>();
        filesystemService = new FileSystemService(fileSystems);
        schedulerService = new SchedulerService(fileSystems);
        server = builder
                .addService(filesystemService)
                .addService(schedulerService)
                .addService(ProtoReflectionService.newInstance())
                .build();
    }

    void parseArgs(String[] args) throws ArgumentParserException {
        Namespace res = parser.parseArgs(args);
        if (res.getBoolean("proto")) {
            printProto();
        }
        port = res.getInt("port");
        serverCertChain = optionalFileArgument(res, "server_cert_chain");
        serverPrivateKey = optionalFileArgument(res, "server_private_key");
        clientCertChain = optionalFileArgument(res, "client_cert_chain");
        useTLS = (serverCertChain != null && serverPrivateKey != null && clientCertChain != null);
        boolean anyTLS = (serverCertChain != null || serverPrivateKey != null || clientCertChain != null);
        if (!useTLS && anyTLS) {
            throw new ArgumentParserException("Unable to enable mutual TLS. mutual TLS requires --server-cert-chain, --server-private-key and --client-cert-chain arguments set", parser);
        }
        configureLogger(res);
    }

    private void configureLogger(Namespace res) {
        Integer verboseness = res.getInt("verbose");
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        if (verboseness == 1) {
            root.setLevel(ch.qos.logback.classic.Level.WARN);
        } else if (verboseness == 2) {
            root.setLevel(ch.qos.logback.classic.Level.INFO);
        } else if (verboseness == 3) {
            root.setLevel(ch.qos.logback.classic.Level.DEBUG);
        } else if (verboseness > 3) {
            root.setLevel(ch.qos.logback.classic.Level.TRACE);
        } else {
            root.setLevel(ch.qos.logback.classic.Level.ERROR);
        }
    }

    private void printProto() {
        InputStream proto = getClass().getResourceAsStream("/xenon.proto");
        try {
            int data = proto.read();
            while (data != -1) {
                System.out.print((char) data);
                data = proto.read();
            }
        } catch (IOException e) {
            LOGGER.warn(e.getMessage(), e);
        }
        System.exit(0);
    }

    private ServerBuilder<?> secureServerBuilder() throws SSLException {
        LOGGER.error("Server started, listening on port {} with mutual TLS", port);
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
        LOGGER.error("Server started, listening on port {}", port);
        return ServerBuilder.forPort(port);
    }

    void stop() {
        if (server != null) {
            server.shutdown();
            try {
                filesystemService.closeAllFileSystems();
                schedulerService.closeAllSchedulers();
            } catch (XenonException e) {
                System.err.println("Unable to close all filesystems and schedulers");
                System.err.println(e.getMessage());
            }
        }
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
