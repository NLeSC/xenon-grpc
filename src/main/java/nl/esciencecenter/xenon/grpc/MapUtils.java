package nl.esciencecenter.xenon.grpc;

import nl.esciencecenter.xenon.XenonPropertyDescription;

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
}
