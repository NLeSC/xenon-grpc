package nl.esciencecenter.xenon.grpc;

import java.util.ArrayList;
import java.util.List;

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
import nl.esciencecenter.xenon.files.OpenOption;

import io.grpc.Status;
import io.grpc.StatusException;

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

    public static OpenOption[] parseOpenOption(List<Integer> optionsList) throws StatusException {
        List<OpenOption> options = new ArrayList<>();
        // OpenOption is only used to write a file, so always add WRITE option.
        options.add(OpenOption.WRITE);
        for (Integer option: optionsList) {
            switch (option) {
                case 0:
                    options.add(OpenOption.CREATE);
                    break;
                case 1:
                    options.add(OpenOption.OPEN);
                    break;
                case 2:
                    options.add(OpenOption.OPEN_OR_CREATE);
                    break;
                case 3:
                    options.add(OpenOption.APPEND);
                    break;
                case 4:
                    options.add(OpenOption.TRUNCATE);
                    break;
            }
        }
        return options.toArray(new OpenOption[0]);
    }

}
