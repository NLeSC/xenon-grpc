package nl.esciencecenter.xenon.grpc;

import io.grpc.StatusException;
import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonFactory;
import nl.esciencecenter.xenon.credentials.Credential;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;

import static nl.esciencecenter.xenon.grpc.Parsers.parseCredential;
import static org.junit.Assert.*;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.hamcrest.core.StringContains.containsString;

public class ParsersTest {
    @Rule
    public TemporaryFolder myfolder = new TemporaryFolder();
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private Xenon xenon;

    @Before
    public void setUp() throws XenonException {
        xenon = XenonFactory.newXenon(new HashMap<>());
    }

    @After
    public void tearDown() throws XenonException {
        XenonFactory.endXenon(xenon);
    }

    @Test
    public void parseCredential_password() throws Exception {
        XenonProto.CertificateCredential cert = XenonProto.CertificateCredential.getDefaultInstance();
        XenonProto.PasswordCredential password = XenonProto.PasswordCredential.newBuilder()
                .setUsername("myusername")
                .setPassword("mypassword")
                .build();
        Credential cred = parseCredential(xenon, "ssh", password, cert);

        assertEquals(cred.getAdaptorName(), "ssh");
        // TODO check username/password have been returned correctly, by mocking xenon
    }

    @Test
    public void parseCredential_cert() throws Exception {
        String fakeCertFile = myfolder.newFile("fakecert.key").getAbsolutePath();
        XenonProto.CertificateCredential cert = XenonProto.CertificateCredential.newBuilder()
                .setCertfile(fakeCertFile)
                .setUsername("myusername")
                .setPassphrase("mypassphrase")
                .build();
        XenonProto.PasswordCredential password = XenonProto.PasswordCredential.getDefaultInstance();

        Credential cred = parseCredential(xenon, "ssh", password, cert);

        assertEquals(cred.getAdaptorName(), "ssh");
        // TODO check certfile,etc have been returned correctly, by mocking xenon
    }

    @Test
    public void parseCredential_certnotfound() throws Exception {
        String fakeCertFile = "/somewhere/that/does/not/exist/fakecert.key";
        XenonProto.CertificateCredential cert = XenonProto.CertificateCredential.newBuilder()
                .setCertfile(fakeCertFile)
                .setUsername("myusername")
                .setPassphrase("mypassphrase")
                .build();
        XenonProto.PasswordCredential password = XenonProto.PasswordCredential.getDefaultInstance();
        thrown.expect(StatusException.class);
        thrown.expectMessage(startsWith("NOT_FOUND"));
        thrown.expectMessage(containsString(fakeCertFile));

        parseCredential(xenon, "ssh", password, cert);
    }
}