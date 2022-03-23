package com.toocol.distribute.encrpt;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.*;
import java.security.KeyFactory;
import java.security.Security;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

/**
 * @author ZhaoZhe (joezane.cn@gmail.com)
 * @date 2022/3/17 11:37
 */
public class PemUtil {

    public static void writeRsaPrivateKey(RSAPrivateKey rsaPrivateKey, String fileName) {
        try {
            PemWriter pemWriter = new PemWriter(new FileWriter(fileName));
            pemWriter.writeObject(new PemObject("RSA PRIVATE KEY", rsaPrivateKey.getEncoded()));
            pemWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeRsaPublicKey(RSAPublicKey rsaPublicKey, String fileName) {
        try {
            PemWriter pemWriter = new PemWriter(new FileWriter(fileName));
            pemWriter.writeObject(new PemObject("RSA PUBLIC KEY", rsaPublicKey.getEncoded()));
            pemWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static RSAPrivateKey readRsaPrivateKey(String fileName) {
        Security.addProvider(new BouncyCastleProvider());
        try {
            KeyFactory factory = KeyFactory.getInstance("RSA", "BC");

            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
            assert inputStream != null;

            PemReader pemReader = new PemReader(new InputStreamReader(inputStream));
            PemObject privatePem = pemReader.readPemObject();
            PKCS8EncodedKeySpec privateSpec = new PKCS8EncodedKeySpec(privatePem.getContent());
            return (RSAPrivateKey) factory.generatePrivate(privateSpec);
        } catch (Exception e) {
            throw new RuntimeException("Unable to extract private RAS Key .", e);
        }
    }

    public static RSAPublicKey readRsaPublicKey(String fileName) {
        Security.addProvider(new BouncyCastleProvider());
        try {
            KeyFactory factory = KeyFactory.getInstance("RSA", "BC");

            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
            assert inputStream != null;

            PemReader pemReader = new PemReader(new InputStreamReader(inputStream));
            PemObject publicPem = pemReader.readPemObject();
            X509EncodedKeySpec publicSpec = new X509EncodedKeySpec(publicPem.getContent());
            return (RSAPublicKey) factory.generatePublic(publicSpec);
        } catch (Exception e) {
            throw new RuntimeException("Unable to extract public RAS Key .", e);
        }
    }

}
