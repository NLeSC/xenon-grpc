package nl.esciencecenter.xenon.grpc.files;

import com.google.protobuf.Descriptors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.*;
import nl.esciencecenter.xenon.credentials.CertificateNotFoundException;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.Credentials;
import nl.esciencecenter.xenon.files.FileSystem;
import nl.esciencecenter.xenon.files.Files;
import nl.esciencecenter.xenon.grpc.XenonFilesGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSingleton;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FilesService extends XenonFilesGrpc.XenonFilesImplBase {
    private final XenonSingleton singleton;
    private Map<String, FileSystemContainer> fileSystems = new ConcurrentHashMap<>();

    public FilesService(XenonSingleton singleton) {
        super();
        this.singleton = singleton;
    }

    @Override
    public void newFileSystem(XenonProto.NewFileSystemRequest request, StreamObserver<XenonProto.FileSystem> responseObserver) {
        Files files = singleton.getInstance().files();
        Credentials creds = singleton.getInstance().credentials();

        XenonProto.CertificateCredential certificate = request.getCertificate();
        XenonProto.PasswordCredential password = request.getPassword();
        try {
            Credential credential;
            // TODO should password/passphrase be masked?
            XenonProto.NewFileSystemRequest.Builder maskedRequest = request.toBuilder();
            if (certificate != null) {
                // TODO use simpler constructor when Xenon 2.0 is released
                credential = creds.newCertificateCredential(null, certificate.getCertfile(), null, certificate.getPassphrase().toCharArray(), null);
                maskedRequest = maskedRequest.setCertificate(certificate.toBuilder().setPassphrase("******"));
            } else if (password != null) {
                // TODO use simpler constructor when Xenon 2.0 is released
                credential = creds.newPasswordCredential(null, password.getUsername(), password.getPassword().toCharArray(), null);
                maskedRequest = maskedRequest.setPassword(password.toBuilder().setPassword("******"));
            } else {
                // TODO remove when Xenon 2.0 is released
                credential = creds.getDefaultCredential(null);
            }
            FileSystem fileSystem = files.newFileSystem(request.getAdaptor(), request.getAdaptor(), credential, request.getPropertiesMap());
            // TODO use more unique id, maybe use new label/alias field from request or a uuid
            String id = fileSystem.getAdaptorName() + ":" + fileSystem.getLocation();
            fileSystems.put(id, new FileSystemContainer(request, fileSystem));

            responseObserver.onNext(XenonProto.FileSystem.newBuilder()
                .setId(id)
                .setRequest(maskedRequest)
                .build()
            );
            responseObserver.onCompleted();
        } catch (CertificateNotFoundException | InvalidLocationException | InvalidSchemeException | InvalidCredentialException | InvalidPropertyException | UnknownPropertyException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void listFileSystems(XenonProto.Empty request, StreamObserver<XenonProto.FileSystems> responseObserver) {
        XenonProto.FileSystems.Builder setBuilder = XenonProto.FileSystems.newBuilder();
        XenonProto.FileSystem.Builder builder = XenonProto.FileSystem.newBuilder();
        for (Map.Entry<String, FileSystemContainer> entry : fileSystems.entrySet()) {
            XenonProto.NewFileSystemRequest fileSystemRequest = entry.getValue().getRequest();
            setBuilder.addFilesystems(builder
                    .setId(entry.getKey())
                    .setRequest(fileSystemRequest)
            );
        }
        responseObserver.onNext(setBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void close(XenonProto.FileSystem request, StreamObserver<XenonProto.Empty> responseObserver) {
        String id = request.getId();
        if (fileSystems.containsKey(id)) {
            responseObserver.onError(Status.NOT_FOUND.asException());
        }
        FileSystem filesystem = fileSystems.get(id).getFileSystem();
        try {
            singleton.getInstance().files().close(filesystem);
            fileSystems.remove(id);
        } catch (XenonException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asException());
        }
        responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
