package com.toocol.distribute.encrpt;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


/**
 * @author zhaozhe
 */
public class EncryptUtils {

    public static String md5(String txt) throws NoSuchAlgorithmException {
        return encrypt(txt, "MD5");
    }

    public static String sha(String txt) throws NoSuchAlgorithmException {
        return encrypt(txt, "SHA");
    }

    public static String sha1(String txt) throws NoSuchAlgorithmException {
        return encrypt(txt, "SHA1");
    }

    private static String encrypt(String txt, String algorithmName) throws NoSuchAlgorithmException {
        if (txt == null || txt.trim().length() == 0) {
            return null;
        }

        if (algorithmName == null || algorithmName.trim().length() == 0) {
            algorithmName = "MD5";
        }

        String result = null;

        MessageDigest m = MessageDigest.getInstance(algorithmName);
        m.reset();
        m.update(txt.getBytes(StandardCharsets.UTF_8));
        byte[] bts = m.digest();

        return hex(bts);
    }

    private static String hex(byte[] bts) {
        StringBuilder sb = new StringBuilder();
        for (byte bt : bts) {
            sb.append(Integer.toHexString((bt & 0xFF) | 0x100), 1, 3);
        }

        return sb.toString();
    }

}
