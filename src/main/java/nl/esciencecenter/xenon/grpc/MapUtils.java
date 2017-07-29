package nl.esciencecenter.xenon.grpc;

import nl.esciencecenter.xenon.XenonPropertyDescription;
import nl.esciencecenter.xenon.credentials.CertificateCredential;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.DefaultCredential;
import nl.esciencecenter.xenon.credentials.PasswordCredential;

import java.util.ArrayList;
import java.util.List;

public class MapUtils {
    private MapUtils() {
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
        return mapCredential(request.getDefaultCred(), request.getPasswordCred(), request.getCertificateCred());
    }

    public static Credential mapCredential(XenonProto.CreateFileSystemRequest request) {
        return mapCredential(request.getDefaultCred(), request.getPasswordCred(), request.getCertificateCred());
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
}
