package com.bteshome.keyvaluestore.storage.common;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Slf4j
public class ChecksumUtil {
    public static void readAndVerify(String filePath) {
        try {
            String checksumFilePath = filePath + ".md5";
            String readDigest = Files.readString(Path.of(checksumFilePath));
            String generatedDigest = generate(filePath);
            if (!readDigest.equals(generatedDigest)) {
                String errorMessage = "File %s failed checksum verification. Generated: '%s', read: '%s'.".formatted(
                        filePath,
                        generatedDigest,
                        readDigest);
                throw new StorageServerException(errorMessage);
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            String errorMessage = "Error verifying checksum for file '%s'.".formatted(filePath);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public static void generateAndWrite(String filePath) {
        try {
            String checksumFilePath = filePath + ".md5";
            String digest = generate(filePath);
            Files.writeString(Path.of(checksumFilePath), digest);
        } catch (Exception e) {
            String errorMessage = "Error generating checksum for file '%s'.".formatted(filePath);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private static String generate(String filePath) throws NoSuchAlgorithmException, IOException {
        MessageDigest md5Digest = MessageDigest.getInstance("MD5");

        try (FileInputStream fileStream = new FileInputStream(filePath)) {
            byte[] bytes = new byte[1024];
            int bytesRead;
            while ((bytesRead = fileStream.read(bytes)) != -1)
                md5Digest.update(bytes, 0, bytesRead);
        }

        byte[] checksumBytes = md5Digest.digest();
        StringBuilder hexString = new StringBuilder();
        for (byte b : checksumBytes)
            hexString.append(String.format("%02x", b));

        return hexString.toString();
    }
}
