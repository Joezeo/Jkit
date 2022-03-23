package com.toocol.distribute.encrpt;

import com.toocol.distribute.tuple.Tuple2;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

/**
 * @author ZhaoZhe (joezane.cn@gmail.com)
 * @date 2022/3/17 11:37
 */
public class RsaEncryptUtil {
    public static final String KEY_ALGORITHM = "RSA";
    public static final String SIGNATURE_ALGORITHM = "MD5withRSA";
    private static final int MAX_ENCRYPT_BLOCK = 117;
    private static final int MAX_DECRYPT_BLOCK = 128;

    public RsaEncryptUtil() {
    }

    public static Tuple2<RSAPublicKey, RSAPrivateKey> generateKeyPair() throws Exception {
        KeyPairGenerator rsaKeyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        rsaKeyPairGenerator.initialize(1024, new SecureRandom());
        KeyPair keyPair = rsaKeyPairGenerator.generateKeyPair();
        RSAPublicKey publicKey = (RSAPublicKey)keyPair.getPublic();
        RSAPrivateKey privateKey = (RSAPrivateKey)keyPair.getPrivate();
        return new Tuple2(publicKey, privateKey);
    }

    public static RSAPublicKey transferPublic(String publicKey) throws Exception {
        byte[] decodedPublicKey = Base64.decodeBase64(publicKey.getBytes(StandardCharsets.UTF_8));
        return (RSAPublicKey)KeyFactory.getInstance(KEY_ALGORITHM).generatePublic(new X509EncodedKeySpec(decodedPublicKey));
    }

    public static RSAPrivateKey transferPrivate(String privateKey) throws Exception {
        byte[] decodedPrivateKey = Base64.decodeBase64(privateKey.getBytes(StandardCharsets.UTF_8));
        return (RSAPrivateKey)KeyFactory.getInstance(KEY_ALGORITHM).generatePrivate(new PKCS8EncodedKeySpec(decodedPrivateKey));
    }

    public static String encrypt(String data, RSAPublicKey rsaPublicKey) throws Exception {
        Cipher cipher = Cipher.getInstance(KEY_ALGORITHM);
        cipher.init(1, rsaPublicKey);
        byte[] bytesData = data.getBytes(StandardCharsets.UTF_8);
        int inputLen = bytesData.length;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int offSet = 0;

        for(int i = 0; inputLen - offSet > 0; offSet = i * MAX_ENCRYPT_BLOCK) {
            byte[] cache;
            if (inputLen - offSet > MAX_ENCRYPT_BLOCK) {
                cache = cipher.doFinal(bytesData, offSet, MAX_ENCRYPT_BLOCK);
            } else {
                cache = cipher.doFinal(bytesData, offSet, inputLen - offSet);
            }

            out.write(cache, 0, cache.length);
            ++i;
        }

        out.close();
        return new String(Base64.encodeBase64(out.toByteArray()), StandardCharsets.UTF_8);
    }

    public static String encrypt(String data, String publicKey) throws Exception {
        byte[] keyBytes = Base64.decodeBase64(publicKey.getBytes(StandardCharsets.UTF_8));
        X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
        Key publicK = keyFactory.generatePublic(x509KeySpec);
        Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
        cipher.init(1, publicK);
        byte[] bytesData = data.getBytes(StandardCharsets.UTF_8);
        int inputLen = bytesData.length;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int offSet = 0;

        for(int i = 0; inputLen - offSet > 0; offSet = i * MAX_ENCRYPT_BLOCK) {
            byte[] cache;
            if (inputLen - offSet > MAX_ENCRYPT_BLOCK) {
                cache = cipher.doFinal(bytesData, offSet, MAX_ENCRYPT_BLOCK);
            } else {
                cache = cipher.doFinal(bytesData, offSet, inputLen - offSet);
            }

            out.write(cache, 0, cache.length);
            ++i;
        }

        out.close();
        return new String(Base64.encodeBase64(out.toByteArray()), StandardCharsets.UTF_8);
    }

    public static String decrypt(String data, RSAPrivateKey rsaPrivateKey) throws Exception {
        Cipher cipher = Cipher.getInstance(KEY_ALGORITHM);
        cipher.init(2, rsaPrivateKey);
        byte[] encryptedData = Base64.decodeBase64(data.getBytes(StandardCharsets.UTF_8));
        int inputLen = encryptedData.length;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int offSet = 0;

        for(int i = 0; inputLen - offSet > 0; offSet = i * MAX_DECRYPT_BLOCK) {
            byte[] cache;
            if (inputLen - offSet > MAX_DECRYPT_BLOCK) {
                cache = cipher.doFinal(encryptedData, offSet, MAX_DECRYPT_BLOCK);
            } else {
                cache = cipher.doFinal(encryptedData, offSet, inputLen - offSet);
            }

            out.write(cache, 0, cache.length);
            ++i;
        }

        out.close();
        return out.toString();
    }

    public static String decrypt(String data, String privateKey) throws Exception {
        byte[] keyBytes = Base64.decodeBase64(privateKey.getBytes(StandardCharsets.UTF_8));
        PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
        Key privateK = keyFactory.generatePrivate(pkcs8KeySpec);
        Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
        cipher.init(2, privateK);
        byte[] encryptedData = Base64.decodeBase64(data.getBytes(StandardCharsets.UTF_8));
        int inputLen = encryptedData.length;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int offSet = 0;

        for(int i = 0; inputLen - offSet > 0; offSet = i * MAX_DECRYPT_BLOCK) {
            byte[] cache;
            if (inputLen - offSet > MAX_DECRYPT_BLOCK) {
                cache = cipher.doFinal(encryptedData, offSet, MAX_DECRYPT_BLOCK);
            } else {
                cache = cipher.doFinal(encryptedData, offSet, inputLen - offSet);
            }

            out.write(cache, 0, cache.length);
            ++i;
        }

        out.close();
        return out.toString();
    }

    public static String signature(RSAPrivateKey rsaPrivateKey) throws Exception {
        Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
        signature.initSign(rsaPrivateKey);
        return new String(Base64.encodeBase64(signature.sign()), StandardCharsets.UTF_8);
    }

    public static String signature(String privateKey) throws Exception {
        byte[] decodedPrivateKey = Base64.decodeBase64(privateKey.getBytes(StandardCharsets.UTF_8));
        RSAPrivateKey rsaPrivateKey = (RSAPrivateKey)KeyFactory.getInstance(KEY_ALGORITHM).generatePrivate(new PKCS8EncodedKeySpec(decodedPrivateKey));
        Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
        signature.initSign(rsaPrivateKey);
        return new String(Base64.encodeBase64(signature.sign()), StandardCharsets.UTF_8);
    }

    public static boolean verifySignature(String code, RSAPublicKey rsaPublicKey) throws Exception {
        Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
        signature.initVerify(rsaPublicKey);
        return signature.verify(Base64.decodeBase64(code.getBytes(StandardCharsets.UTF_8)));
    }

    public static boolean verifySignature(String code, String publicKey) throws Exception {
        byte[] decodedPublicKey = Base64.decodeBase64(publicKey.getBytes(StandardCharsets.UTF_8));
        RSAPublicKey rsaPublicKey = (RSAPublicKey)KeyFactory.getInstance(KEY_ALGORITHM).generatePublic(new X509EncodedKeySpec(decodedPublicKey));
        Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
        signature.initVerify(rsaPublicKey);
        return signature.verify(Base64.decodeBase64(code.getBytes(StandardCharsets.UTF_8)));
    }
}
