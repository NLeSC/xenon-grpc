package nl.esciencecenter.xenon.grpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        return mapCredential(request.getDefaultCredential(), request.getPasswordCredential(), request.getCertificateCredential());
    }

    public static Credential mapCredential(XenonProto.CreateFileSystemRequest request) {
        return mapCredential(request.getDefaultCredential(), request.getPasswordCredential(), request.getCertificateCredential());
    }

    private static Credential mapCredential(XenonProto.DefaultCredential defaultCred, XenonProto.PasswordCredential passwordCred, XenonProto.CertificateCredential certificateCred) {
        Credential credential;
        // if embedded message is not set then the request field will have the default instance,
        if (!XenonProto.CertificateCredential.getDefaultInstance().equals(certificateCred)) {
            credential = new CertificateCredential(certificateCred.getUsername(), certificateCred.getCertfile(), certificateCred.getPassphrase().toCharArray());
        } else if (!XenonProto.PasswordCredential.getDefaultInstance().equals(passwordCred)) {
            credential = new PasswordCredential(passwordCred.getUsername(), passwordCred.getPassword().toCharArray());
        } else if (!XenonProto.DefaultCredential.getDefaultInstance().equals(defaultCred)) {
            credential = new DefaultCredential(defaultCred.getUsername());
        } else {
            credential = new DefaultCredential();
        }
        return credential;
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
        return s.withDescription(e.getMessage()).withCause(e).asException();
    }

    public static String usernameOfCredential(Credential credential) {
        if (credential instanceof UserCredential) {
            return ((UserCredential) credential).getUsername();
        } else {
            return "nousername";
        }
    }
}
