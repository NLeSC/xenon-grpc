package nl.esciencecenter.xenon.grpc;

import io.grpc.Status;
import io.grpc.StatusException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.credentials.CertificateCredential;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.DefaultCredential;
import nl.esciencecenter.xenon.credentials.PasswordCredential;

/**
 * Parse grpc messages to Xenon objects
 */
public class Parsers {
    private Parsers() {
    }

    public static Credential parseCredential(XenonProto.DefaultCredential defaultCred, XenonProto.PasswordCredential password, XenonProto.CertificateCredential certificate) throws StatusException, XenonException {
        Credential credential;
        // if embedded message is not set then the request field will have the default instance,
        if (!XenonProto.CertificateCredential.getDefaultInstance().equals(certificate)) {
            credential = new CertificateCredential(certificate.getUsername(), certificate.getCertfile(), certificate.getPassphrase().toCharArray());
        } else if (!XenonProto.PasswordCredential.getDefaultInstance().equals(password)) {
            credential = new PasswordCredential(password.getUsername(), password.getPassword().toCharArray());
        } else if (!XenonProto.DefaultCredential.getDefaultInstance().equals(defaultCred)) {
            credential = new DefaultCredential(defaultCred.getUsername());
        } else {
            throw Status.INVALID_ARGUMENT.withDescription("defaultCred, password or certicate should be filled").asException();
        }
        return credential;
    }
}
