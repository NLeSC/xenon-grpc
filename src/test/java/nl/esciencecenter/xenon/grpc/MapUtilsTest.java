package nl.esciencecenter.xenon.grpc;

import nl.esciencecenter.xenon.XenonPropertyDescription;
import nl.esciencecenter.xenon.credentials.CertificateCredential;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.DefaultCredential;
import nl.esciencecenter.xenon.credentials.PasswordCredential;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static nl.esciencecenter.xenon.grpc.MapUtils.*;
import static org.junit.Assert.assertEquals;

public class MapUtilsTest {
    @Test
    public void mapPropertyDescriptions_booltype() throws Exception {
        XenonPropertyDescription[] input = new XenonPropertyDescription[] {
            new XenonPropertyDescription("abool", XenonPropertyDescription.Type.BOOLEAN, "false", "abool desc")
        };

        List<XenonProto.PropertyDescription> result = mapPropertyDescriptions(input);

        List<XenonProto.PropertyDescription> expected = Collections.singletonList(
                XenonProto.PropertyDescription.newBuilder()
                        .setName("abool")
                        .setType(XenonProto.PropertyDescription.Type.BOOLEAN)
                        .setDefaultValue("false")
                        .setDescription("abool desc")
                        .build()
        );
        assertEquals(expected, result);
    }

    @Test
    public void test_empty() throws Exception {
        XenonProto.Empty message = empty();

        XenonProto.Empty expected = XenonProto.Empty.getDefaultInstance();
        assertEquals(expected, message);
    }

    @Test
    public void mapCredential_fileSystem_default() throws Exception {
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .build();

        Credential result = mapCredential(request);

        DefaultCredential expected = new DefaultCredential();
        assertEquals(expected, result);
    }

    @Test
    public void mapCredential_scheduler_username() throws Exception {
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setDefaultCred(XenonProto.DefaultCredential.newBuilder().setUsername("someone"))
                .build();

        Credential result = mapCredential(request);

        Credential expected = new DefaultCredential("someone");
        assertEquals(expected, result);
    }

    @Test
    public void mapCredential_scheduler_usernamePassword() throws Exception {
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setPasswordCred(XenonProto.PasswordCredential.newBuilder().setUsername("someone").setPassword("mypassword"))
                .build();

        Credential result = mapCredential(request);

        Credential expected = new PasswordCredential("someone", "mypassword".toCharArray());
        assertEquals(expected, result);
    }

    @Test
    public void mapCredential_scheduler_certificate() throws Exception {
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCertificateCred(XenonProto.CertificateCredential.newBuilder()
                        .setUsername("someone")
                        .setCertfile("/home/someone/.ssh/id_rsa")
                        .setPassphrase("mypassphrase")
                )
                .build();

        Credential result = mapCredential(request);

        Credential expected = new CertificateCredential("someone","/home/someone/.ssh/id_rsa", "mypassphrase".toCharArray());
        assertEquals(expected, result);
    }

    @Test
    public void mapCredential_filesystem_default() throws Exception {
        XenonProto.CreateFileSystemRequest request = XenonProto.CreateFileSystemRequest.newBuilder()
                .setAdaptor("file")
                .build();

        Credential result = mapCredential(request);

        DefaultCredential expected = new DefaultCredential();
        assertEquals(expected, result);
    }


}