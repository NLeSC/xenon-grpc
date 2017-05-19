package nl.esciencecenter.xenon.grpc;

import io.grpc.Status;
import io.grpc.StatusException;
import nl.esciencecenter.xenon.*;
import nl.esciencecenter.xenon.credentials.CertificateNotFoundException;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.Credentials;

/**
 * Parse grpc messages to Xenon objects
 */
public class Parsers {
    public static Credential parseCredential(Xenon xenon, XenonProto.PasswordCredential password, XenonProto.CertificateCredential certificate) throws StatusException, XenonException {
        Credentials creds = xenon.credentials();
        try {
            Credential credential;
            String dummyScheme = "local";
            // if embedded message is not set then the request field will have the default instance,
            if (!XenonProto.CertificateCredential.getDefaultInstance().equals(certificate)) {
                // TODO use simpler constructor when Xenon 2.0 is released
                credential = creds.newCertificateCredential(dummyScheme, certificate.getCertfile(), null, certificate.getPassphrase().toCharArray(), null);
            } else if (!XenonProto.PasswordCredential.getDefaultInstance().equals(password)) {
                // TODO use simpler constructor when Xenon 2.0 is released
                credential = creds.newPasswordCredential(dummyScheme, password.getUsername(), password.getPassword().toCharArray(), null);
            } else {
                // TODO remove when Xenon 2.0 is released
                credential = creds.getDefaultCredential(dummyScheme);
            }
            return credential;
        } catch (CertificateNotFoundException | InvalidLocationException | InvalidSchemeException | InvalidCredentialException | InvalidPropertyException | UnknownPropertyException e) {
            throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asException();
        }
    }
}
