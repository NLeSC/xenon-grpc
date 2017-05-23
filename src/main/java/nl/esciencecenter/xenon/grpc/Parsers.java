package nl.esciencecenter.xenon.grpc;

import nl.esciencecenter.xenon.InvalidCredentialException;
import nl.esciencecenter.xenon.InvalidLocationException;
import nl.esciencecenter.xenon.InvalidPropertyException;
import nl.esciencecenter.xenon.InvalidSchemeException;
import nl.esciencecenter.xenon.UnknownPropertyException;
import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.credentials.CertificateNotFoundException;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.Credentials;

import io.grpc.Status;
import io.grpc.StatusException;

/**
 * Parse grpc messages to Xenon objects
 */
public class Parsers {
    private Parsers() {
    }

    public static Credential parseCredential(Xenon xenon, String scheme, XenonProto.PasswordCredential password, XenonProto.CertificateCredential certificate) throws StatusException, XenonException {
        Credentials creds = xenon.credentials();
        try {
            Credential credential;
            // if embedded message is not set then the request field will have the default instance,
            if (!XenonProto.CertificateCredential.getDefaultInstance().equals(certificate)) {
                // TODO use simpler constructor when Xenon 2.0 is released
                credential = creds.newCertificateCredential(scheme, certificate.getCertfile(), null, certificate.getPassphrase().toCharArray(), null);
            } else if (!XenonProto.PasswordCredential.getDefaultInstance().equals(password)) {
                // TODO use simpler constructor when Xenon 2.0 is released
                credential = creds.newPasswordCredential(scheme, password.getUsername(), password.getPassword().toCharArray(), null);
            } else {
                // TODO remove when Xenon 2.0 is released
                credential = creds.getDefaultCredential(scheme);
            }
            return credential;
        } catch (CertificateNotFoundException | InvalidLocationException | InvalidSchemeException | InvalidCredentialException | InvalidPropertyException | UnknownPropertyException e) {
            throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asException();
        }
    }
}
