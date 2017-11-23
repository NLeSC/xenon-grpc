package nl.esciencecenter.xenon.grpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.grpc.Status;
import io.grpc.StatusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.esciencecenter.xenon.InvalidCredentialException;
import nl.esciencecenter.xenon.InvalidLocationException;
import nl.esciencecenter.xenon.InvalidPropertyException;
import nl.esciencecenter.xenon.UnknownAdaptorException;
import nl.esciencecenter.xenon.UnknownPropertyException;
import nl.esciencecenter.xenon.UnsupportedOperationException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonPropertyDescription;
import nl.esciencecenter.xenon.adaptors.NotConnectedException;
import nl.esciencecenter.xenon.credentials.CertificateCredential;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.CredentialMap;
import nl.esciencecenter.xenon.credentials.DefaultCredential;
import nl.esciencecenter.xenon.credentials.PasswordCredential;
import nl.esciencecenter.xenon.credentials.UserCredential;
import nl.esciencecenter.xenon.filesystems.InvalidPathException;
import nl.esciencecenter.xenon.filesystems.NoSuchPathException;
import nl.esciencecenter.xenon.filesystems.PathAlreadyExistsException;
import nl.esciencecenter.xenon.schedulers.IncompleteJobDescriptionException;
import nl.esciencecenter.xenon.schedulers.InvalidJobDescriptionException;
import nl.esciencecenter.xenon.schedulers.NoSuchQueueException;
import nl.esciencecenter.xenon.schedulers.UnsupportedJobDescriptionException;

public class MapUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapUtils.class);

    private MapUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static List<XenonProto.PropertyDescription> mapPropertyDescriptions(XenonPropertyDescription[] props) {
        List<XenonProto.PropertyDescription> supportedProperties = new ArrayList<>();
        XenonProto.PropertyDescription.Builder propBuilder = XenonProto.PropertyDescription.newBuilder();
        for (XenonPropertyDescription prop : props) {
            String defaultValue = prop.getDefaultValue();
            if (defaultValue == null) {
                defaultValue = "";
            }

            XenonProto.PropertyDescription.Type type = XenonProto.PropertyDescription.Type.STRING;
            switch (prop.getType()) {
                case BOOLEAN:
                    type = XenonProto.PropertyDescription.Type.BOOLEAN;
                    break;
                case INTEGER:
                    type = XenonProto.PropertyDescription.Type.INTEGER;
                    break;
                case LONG:
                    type = XenonProto.PropertyDescription.Type.LONG;
                    break;
                case DOUBLE:
                    type = XenonProto.PropertyDescription.Type.DOUBLE;
                    break;
                case STRING:
                    type = XenonProto.PropertyDescription.Type.STRING;
                    break;
                case SIZE:
                    type = XenonProto.PropertyDescription.Type.SIZE;
                    break;
                case NATURAL:
                    type = XenonProto.PropertyDescription.Type.NATURAL;
                    break;
            }
            supportedProperties.add(
                propBuilder
                    .setName(prop.getName())
                    .setDescription(prop.getDescription())
                    .setDefaultValue(defaultValue)
                    .setType(type)
                    .build()
            );
        }
        return supportedProperties;
    }

    public static XenonProto.Empty empty() {
        return XenonProto.Empty.getDefaultInstance();
    }

    public static Credential mapCredential(XenonProto.CreateSchedulerRequest request) {
        XenonProto.CreateSchedulerRequest.CredentialCase credCase = request.getCredentialCase();
        switch (credCase) {
            case CERTIFICATE_CREDENTIAL:
                return mapCertificateCredential(request.getCertificateCredential());
            case PASSWORD_CREDENTIAL:
                return mapPasswordCredential(request.getPasswordCredential());
            case DEFAULT_CREDENTIAL:
                return mapDefaultCredential(request.getDefaultCredential());
            case CREDENTIAL_MAP:
                return mapCredentialMap(request.getCredentialMap());
            case CREDENTIAL_NOT_SET:
                break;
        }
        return new DefaultCredential();
    }

    public static Credential mapCredential(XenonProto.CreateFileSystemRequest request) {
        XenonProto.CreateFileSystemRequest.CredentialCase credCase = request.getCredentialCase();
        switch (credCase) {
            case CERTIFICATE_CREDENTIAL:
                return mapCertificateCredential(request.getCertificateCredential());
            case PASSWORD_CREDENTIAL:
                return mapPasswordCredential(request.getPasswordCredential());
            case DEFAULT_CREDENTIAL:
                return mapDefaultCredential(request.getDefaultCredential());
            case CREDENTIAL_MAP:
                return mapCredentialMap(request.getCredentialMap());
            case CREDENTIAL_NOT_SET:
                break;
        }
        return new DefaultCredential();
    }

    private static Credential mapCredentialMap(XenonProto.CredentialMap credentialMap) {
        XenonProto.CredentialMap.FallbackCase fallbackCase = credentialMap.getFallbackCase();
        CredentialMap cred = new CredentialMap();
        switch (fallbackCase) {
            case CERTIFICATE_CREDENTIAL:
                cred = new CredentialMap(mapCertificateCredential(credentialMap.getCertificateCredential()));
                break;
            case PASSWORD_CREDENTIAL:
                cred = new CredentialMap(mapPasswordCredential(credentialMap.getPasswordCredential()));
                break;
            case DEFAULT_CREDENTIAL:
                cred = new CredentialMap(mapDefaultCredential(credentialMap.getDefaultCredential()));
                break;
            case FALLBACK_NOT_SET:
                break;
        }

        for (Map.Entry<String, XenonProto.UserCredential> entry : credentialMap.getEntriesMap().entrySet()) {
            cred.put(entry.getKey(), mapUserCredential(entry.getValue()));
        }

        return cred;
    }

    private static UserCredential mapUserCredential(XenonProto.UserCredential value) {
        XenonProto.UserCredential.EntryCase entryCase = value.getEntryCase();
        switch (entryCase) {
            case CERTIFICATE_CREDENTIAL:
                return mapCertificateCredential(value.getCertificateCredential());
            case PASSWORD_CREDENTIAL:
                return mapPasswordCredential(value.getPasswordCredential());
            case DEFAULT_CREDENTIAL:
                return mapDefaultCredential(value.getDefaultCredential());
            case ENTRY_NOT_SET:
                break;
        }
        return new DefaultCredential();
    }

    private static UserCredential mapDefaultCredential(XenonProto.DefaultCredential defaultCred) {
        if (XenonProto.DefaultCredential.getDefaultInstance().equals(defaultCred)) {
            return new DefaultCredential();
        }
        return new DefaultCredential(defaultCred.getUsername());
    }

    private static UserCredential mapPasswordCredential(XenonProto.PasswordCredential passwordCred) {
        return new PasswordCredential(passwordCred.getUsername(), passwordCred.getPassword().toCharArray());
    }

    private static UserCredential mapCertificateCredential(XenonProto.CertificateCredential certificateCred) {
        return new CertificateCredential(certificateCred.getUsername(), certificateCred.getCertfile(), certificateCred.getPassphrase().toCharArray());
    }

    public static XenonProto.GetCredentialResponse toCredentialResponse(Credential credential) throws XenonException {
        XenonProto.GetCredentialResponse.Builder builder = XenonProto.GetCredentialResponse.newBuilder();
        if (credential instanceof CredentialMap) {
            builder.setCredentialMap(mapCredentialMap((CredentialMap) credential));
        } else if (credential instanceof CertificateCredential) {
            builder.setCertificateCredential(mapCertificateCredential((CertificateCredential) credential));
        } else if (credential instanceof PasswordCredential) {
            builder.setPasswordCredential(mapPasswordCredential((PasswordCredential) credential));
        } else if (credential instanceof DefaultCredential) {
            builder.setDefaultCredential(mapDefaultCredential((DefaultCredential) credential));
        } else {
            return catchUnknownCredential();
        }
        return builder.build();
    }

    private static XenonProto.GetCredentialResponse catchUnknownCredential() throws XenonException {
        throw new XenonException("credential", "Unknown credential class");
    }

    private static XenonProto.DefaultCredential mapDefaultCredential(DefaultCredential credential) {
        return XenonProto.DefaultCredential.newBuilder().setUsername(credential.getUsername()).build();
    }

    private static XenonProto.PasswordCredential mapPasswordCredential(PasswordCredential credential) {
        return XenonProto.PasswordCredential.newBuilder().setUsername(credential.getUsername()).setPassword(String.valueOf(credential.getPassword())).build();
    }

    private static XenonProto.CertificateCredential mapCertificateCredential(CertificateCredential credential) {
        return XenonProto.CertificateCredential.newBuilder()
            .setCertfile(credential.getCertificateFile())
            .setUsername(credential.getUsername())
            .setPassphrase(String.valueOf(credential.getPassword()))
            .build();
    }

    private static XenonProto.CredentialMap mapCredentialMap(CredentialMap credential) throws XenonException {
        XenonProto.CredentialMap.Builder builder = XenonProto.CredentialMap.newBuilder();
        UserCredential defaultCred = credential.getDefault();
        if (defaultCred != null) {
            if (defaultCred instanceof CertificateCredential) {
                builder.setCertificateCredential(mapCertificateCredential((CertificateCredential) defaultCred));
            } else if (defaultCred instanceof PasswordCredential) {
                builder.setPasswordCredential(mapPasswordCredential((PasswordCredential) defaultCred));
            } else if (defaultCred instanceof DefaultCredential) {
                builder.setDefaultCredential(mapDefaultCredential((DefaultCredential) defaultCred));
            } else {
                catchUnknownCredential();
            }
        }
        for (String key : credential.keySet()) {
            builder.putEntries(key, mapUserCredential(credential.get(key)));
        }
        return builder.build();
    }

    private static XenonProto.UserCredential mapUserCredential(UserCredential credential) throws XenonException {
        XenonProto.UserCredential.Builder builder = XenonProto.UserCredential.newBuilder();
        if (credential instanceof CertificateCredential) {
            builder.setCertificateCredential(mapCertificateCredential((CertificateCredential) credential));
        } else if (credential instanceof PasswordCredential) {
            builder.setPasswordCredential(mapPasswordCredential((PasswordCredential) credential));
        } else if (credential instanceof DefaultCredential) {
            builder.setDefaultCredential(mapDefaultCredential((DefaultCredential) credential));
        } else {
            catchUnknownCredential();
        }
        return builder.build();
    }

    public static StatusException mapException(Throwable e) {
        Status s;
        if (e instanceof StatusException) {
            return (StatusException) e;
        } else if (e instanceof NoSuchPathException ||
            e instanceof UnknownAdaptorException ||
            e instanceof UnknownPropertyException ||
            e instanceof NoSuchQueueException
            ) {
            s = Status.NOT_FOUND;
        } else if (e instanceof PathAlreadyExistsException) {
            s = Status.ALREADY_EXISTS;
        } else if (e instanceof UnsupportedOperationException ||
            e instanceof UnsupportedJobDescriptionException
            ) {
            s = Status.UNIMPLEMENTED;
        } else if (e instanceof IllegalArgumentException ||
            e instanceof IncompleteJobDescriptionException) {
            s = Status.INVALID_ARGUMENT;
        } else if (e instanceof NotConnectedException) {
            s = Status.UNAVAILABLE;
        } else if (e instanceof InvalidPathException ||
            e instanceof InvalidPropertyException ||
            e instanceof InvalidCredentialException ||
            e instanceof InvalidLocationException ||
            e instanceof InvalidJobDescriptionException) {
            s = Status.FAILED_PRECONDITION;
        } else if (e instanceof XenonException || e instanceof IOException) {
            s = Status.INTERNAL;
        } else {
            LOGGER.debug("Unable to map exception failing back to INTERNAL status code", e);
            s = Status.INTERNAL;
        }
        return s.withDescription(e.getClass().getName() + ": " + e.getMessage()).withCause(e).asException();
    }

    public static String usernameOfCredential(Credential credential) {
        if (credential instanceof UserCredential) {
            return ((UserCredential) credential).getUsername();
        } else {
            return "nousername";
        }
    }
}
