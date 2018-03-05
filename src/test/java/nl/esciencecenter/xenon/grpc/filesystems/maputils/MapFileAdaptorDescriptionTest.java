package nl.esciencecenter.xenon.grpc.filesystems.maputils;

import nl.esciencecenter.xenon.XenonPropertyDescription;
import nl.esciencecenter.xenon.credentials.DefaultCredential;
import nl.esciencecenter.xenon.filesystems.FileSystemAdaptorDescription;
import nl.esciencecenter.xenon.grpc.XenonProto;
import org.junit.Test;

import java.util.List;

import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.mapFileAdaptorDescription;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class MapFileAdaptorDescriptionTest {

    /**
     * Description which has no values the same as the proto default
     */
    class MockDescription implements FileSystemAdaptorDescription {

        @Override
        public boolean supportsThirdPartyCopy() {
            return true;
        }

        @Override
        public boolean canReadSymboliclinks() {
            return true;
        }

        @Override
        public boolean canCreateSymboliclinks() {
            return true;
        }

        @Override
        public boolean isConnectionless() {
            return true;
        }

        @Override
        public boolean supportsReadingPosixPermissions() {
            return true;
        }

        @Override
        public boolean supportsSettingPosixPermissions() {
            return true;
        }

        @Override
        public boolean supportsRename() {
            return true;
        }

        @Override
        public boolean canAppend() {
            return true;
        }

        @Override
        public boolean needsSizeBeforehand() {
            return true;
        }

        @Override
        public String getName() {
            return "mock";
        }

        @Override
        public String getDescription() {
            return "Some description";
        }

        @Override
        public String[] getSupportedLocations() {
            return new String[]{"host"};
        }

        @Override
        public Class[] getSupportedCredentials() {
            return new Class[]{DefaultCredential.class};
        }

        @Override
        public XenonPropertyDescription[] getSupportedProperties() {
            return new XenonPropertyDescription[]{
                    new XenonPropertyDescription("propname", XenonPropertyDescription.Type.INTEGER, "42", "Mock prop desc")
            };
        }
    }

    @Test
    public void test_withMockedDescription() {
        FileSystemAdaptorDescription xenon_description = new MockDescription();

        XenonProto.FileSystemAdaptorDescription grpc_description = mapFileAdaptorDescription(xenon_description);

        assertTrue("supports_third_party_copy", grpc_description.getSupportsThirdPartyCopy());
        assertTrue("can_read_symboliclinks", grpc_description.getCanReadSymboliclinks());
        assertTrue("can_create_symboliclinks", grpc_description.getCanCreateSymboliclinks());
        assertTrue("is_connectionless", grpc_description.getIsConnectionless());
        assertTrue("supports_reading_posix_permissions", grpc_description.getSupportsReadingPosixPermissions());
        assertTrue("supports_setting_posix_permissions", grpc_description.getSupportsSettingPosixPermissions());
        assertTrue("supports_rename", grpc_description.getSupportsRename());
        assertTrue("can_append", grpc_description.getCanAppend());
        assertTrue("needs_size_beforehand", grpc_description.getNeedsSizeBeforehand());
        assertEquals("mock", grpc_description.getName());
        assertEquals("Some description", grpc_description.getDescription());
        assertArrayEquals(new String[]{"host"}, grpc_description.getSupportedLocationsList().toArray());
        assertArrayEquals(new String[]{"DefaultCredential"}, grpc_description.getSupportedCredentialsList().toArray());
        List<XenonProto.PropertyDescription> expectedPropDesc = XenonProto.PropertyDescriptions.newBuilder().addProperties(
                XenonProto.PropertyDescription.newBuilder()
                        .setName("propname")
                        .setType(XenonProto.PropertyDescription.Type.INTEGER)
                        .setDefaultValue("42")
                        .setDescription("Mock prop desc")
        ).build().getPropertiesList();
        assertEquals(expectedPropDesc, grpc_description.getSupportedPropertiesList());
    }
}