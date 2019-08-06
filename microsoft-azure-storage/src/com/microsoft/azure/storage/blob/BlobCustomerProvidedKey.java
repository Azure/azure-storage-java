package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.core.Base64;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Immutable wrapper for an encryption key to be used with client provided key encryption.
 */
public final class BlobCustomerProvidedKey {

    /**
     * Base64 encoded string of the encryption key.
     */
    private final String key;

    /**
     * Base64 encoded string of the encryption key's SHA256 hash.
     */
    private final String keySHA256;

    /**
     * The algorithm for Azure Blob Storage to encrypt with.
     * Azure Blob Storage only offers AES256 encryption.
     */
    private final String encryptionAlgorithm = "AES256";


    /**
     * Creates a new wrapper for a client provided key.
     *
     * @param key
     *          The encryption key encoded as a base64 string.
     *
     * @throws NoSuchAlgorithmException
     *          Throws if MessageDigest "SHA-256" cannot be found.
     */
    public BlobCustomerProvidedKey(String key) throws NoSuchAlgorithmException {

        this.key = key;

        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] encodedHash = sha256.digest(Base64.decode(key));
        this.keySHA256 = Base64.encode(encodedHash);
    }

    /**
     * Creates a new wrapper for a client provided key.
     *
     * @param key
     *          The encryption key bytes.
     *
     * @throws NoSuchAlgorithmException
     *          Throws if MessageDigest "SHA-256" cannot be found.
     */
    public BlobCustomerProvidedKey(byte[] key) throws NoSuchAlgorithmException {

        this.key = Base64.encode(key);

        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] encodedHash = sha256.digest(key);
        this.keySHA256 = Base64.encode(encodedHash);
    }


    /**
     * Gets the encryption key.
     *
     * @return
     *          A base64 encoded string of the encryption key.
     */
    public String getKey() {
        return key;
    }

    /**
     * Gets the encryption key's hash.
     *
     * @return
     *          A base64 encoded string of the encryption key hash.
     */
    public String getKeySHA256() {
        return keySHA256;
    }

    /**
     * Gets the algorithm to use this key with.
     *
     * @return
     *          A label for the encryption algorithm, as understood by Azure Storage.
     */
    public String getEncryptionAlgorithm() {
        return encryptionAlgorithm;
    }
}
