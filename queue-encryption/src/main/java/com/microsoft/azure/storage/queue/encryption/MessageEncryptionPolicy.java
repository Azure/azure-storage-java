/*
 * Copyright Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.azure.storage.queue.encryption;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.keyvault.core.IKey;
import com.microsoft.azure.keyvault.core.IKeyResolver;
import io.reactivex.Single;


import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Base64;
import java.util.Map;

/**
 * Represents a message encryption policy that is used to perform envelope encryption/decryption of Azure queue messages.
 */
public final class MessageEncryptionPolicy {

    // IKey and IKeyResolver are intentionally made final because MessageEncryptionPolicy needs to be thread safe.
    /**
     * An object of type {@link IKey} that is used to wrap/unwrap the content key during encryption.
     */
    private final IKey keyWrapper;

    /**
     * The {@link IKeyResolver} used to select the correct key for decrypting existing queue messages.
     */
    private final IKeyResolver keyResolver;

    /**
     * Initializes a new instance of the {@link MessageEncryptionPolicy} class with the specified key and resolver.
     * <p>
     * If the generated policy is intended to be used for encryption, users are expected to provide a key at the
     * minimum. The absence of key will cause an exception to be thrown during encryption. If the generated policy is
     * intended to be used for decryption, users can provide a keyResolver. The client library will - 1. Invoke the key
     * resolver if specified to get the key. 2. If resolver is not specified but a key is specified, match the key id on
     * the key and use it.
     *
     * @param keyWrapper
     *            An object of type {@link IKey} that is used to wrap/unwrap the content encryption key.
     * @param keyResolver
     *            The key resolver used to select the correct key for decrypting existing queue messages.
     */
    public MessageEncryptionPolicy(IKey keyWrapper, IKeyResolver keyResolver) {
        this.keyWrapper = keyWrapper;
        this.keyResolver = keyResolver;
    }

    /**
     * Return an encrypted base64 encoded message along with encryption related metadata given a plain text message.
     *
     * @param message
     *            The input message as a String.
     * @return The encrypted message that will be uploaded to the service.
     * @throws InvalidKeyException
     *             If provided key is invalid
     */
    public Single<String> encryptMessage(String message) throws InvalidKeyException {
        Utility.assertNotNull(Constants.KEY, this.keyWrapper);
        Utility.assertNotNull(Constants.MESSAGE, message);

        Map<String, String> keyWrappingMetadata = new HashMap<>();
        keyWrappingMetadata.put(Constants.AGENT_METADATA_KEY, Constants.AGENT_METADATA_VALUE);

        EncryptionData encryptionData = new EncryptionData()
                .withEncryptionAgent(new EncryptionAgent(Constants.ENCRYPTION_PROTOCOL_V1, EncryptionAlgorithm.AES_CBC_256))
                .withKeyWrappingMetadata(keyWrappingMetadata);

        QueueEncryptedMessage queueEncryptedMessage = new QueueEncryptedMessage()
                .withEncryptionData(encryptionData);

        try {
            KeyGenerator keyGen = KeyGenerator.getInstance(Constants.AES);
            keyGen.init(Constants.KEY_SIZE_256);

            Cipher aesCipher = Cipher.getInstance(Constants.AES_CBC_PKCS5PADDING);
            SecretKey secretKey = keyGen.generateKey();
            aesCipher.init(Cipher.ENCRYPT_MODE, secretKey);

            // Wrap key
            return Single.fromFuture(this.keyWrapper.wrapKeyAsync(secretKey.getEncoded(), null /* algorithm */))
                    .map(key -> {
                        encryptionData.withWrappedContentKey(new WrappedKey(this.keyWrapper.getKid(), key.getKey(),
                                key.getValue()));

                        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);

                        String encryptedString = Base64.getEncoder().encodeToString(
                                aesCipher.doFinal(messageBytes, 0, messageBytes.length));

                        queueEncryptedMessage.withEncryptedMessageContents(encryptedString);

                        encryptionData.withContentEncryptionIV(aesCipher.getIV());
                        queueEncryptedMessage.withEncryptionData(encryptionData);

                        ObjectMapper objectMapper = new ObjectMapper();
                        String encryptedMessageString = objectMapper.writeValueAsString(queueEncryptedMessage);

                        if(encryptedMessageString.length() > Constants.MAX_MESSAGE_SIZE) {
                            throw new IllegalArgumentException("Encrypted message exceeds maximum queue message size");
                        }

                        return encryptedMessageString;
                    });

        } catch(NoSuchAlgorithmException | NoSuchPaddingException e) {
            // We are hard coding the algorithm and padding, so we don't want to burden the customer with checked exception types here.
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a plain text message given an encrypted message.
     *
     * @param encryptedMessage
     *            The encrypted message.
     * @return The plain text message bytes.
     * @throws IOException
     *             An exception representing any error which occurred during the operation.
     */
    public Single<String> decryptMessage(String encryptedMessage, boolean requireEncryption) throws IOException {
        Utility.assertNotNull(Constants.ENCRYPTED_MESSAGE, encryptedMessage);
        ObjectMapper objectMapper = new ObjectMapper();

        if(!encryptedMessage.contains(Constants.ENCRYPTION_DATA)) {
            if(requireEncryption) {
                throw new IllegalArgumentException(
                        "Encryption data does not exist. If you do not want to decrypt the data, please do not set the require encryption flag to true");
            }
            else {
                return Single.just(encryptedMessage);
            }
        }

        QueueEncryptedMessage queueEncryptedMessage;

        try {
            queueEncryptedMessage = objectMapper.readValue(encryptedMessage, QueueEncryptedMessage.class);
        }
        catch(JsonParseException e) {
            // This block exists for the edge case where the string "EncryptionData" exists in an unencrypted message.
            if(requireEncryption) {
                throw new IllegalArgumentException(
                        "Encryption data does not exist. If you do not want to decrypt the data, please do not set the require encryption flag to true");
            }
            else {
                return Single.just(encryptedMessage);
            }
        }

        EncryptionData encryptionData = queueEncryptedMessage.encryptionData();

        // Throw if the encryption protocol on the message doesn't match the version that this client library understands
        // and is able to decrypt.
        if(!Constants.ENCRYPTION_PROTOCOL_V1.equals(encryptionData.encryptionAgent().protocol())) {
            throw new IllegalArgumentException(String.format(Locale.ROOT,
                    "Invalid Encryption Agent. This version of the client library does not understand the Encryption Agent set on the queue message: %s",
                    encryptionData.encryptionAgent()));
        }

        // Throw if neither the key nor the key resolver are set.
        if(this.keyWrapper == null && this.keyResolver == null) {
            throw new IllegalArgumentException("Key and KeyResolver cannot both be null");
        }

        return getContentEncryptionKey(encryptionData)
                .map(contentEncryptionKey -> {
                    switch (encryptionData.encryptionAgent().algorithm()) {
                        case AES_CBC_256:

                            Cipher myAes = Cipher.getInstance(Constants.AES_CBC_PKCS5PADDING);

                            IvParameterSpec ivParameterSpec = new IvParameterSpec(encryptionData.contentEncryptionIV());
                            SecretKey keySpec = new SecretKeySpec(contentEncryptionKey, 0, contentEncryptionKey.length,
                                    Constants.AES);
                            myAes.init(Cipher.DECRYPT_MODE, keySpec,ivParameterSpec);

                            byte[] src = Base64.getDecoder().decode(queueEncryptedMessage.encryptedMessageContents());
                            byte[] decryptedBytes = myAes.doFinal(src, 0, src.length);
                            return new String(decryptedBytes, 0, decryptedBytes.length, StandardCharsets.UTF_8);


                        default:
                            throw new IllegalArgumentException(
                                    "Invalid Encryption Algorithm found on the resource. This version of the client library does not support the specified encryption algorithm.");
                    }
                });
    }

    private Single<byte[]> getContentEncryptionKey(EncryptionData encryptionData) {
        // 1. Invoke the key resolver if specified to get the key. If the resolver is specified but does not have a
        // mapping for the key id, an error should be thrown. This is important for key rotation scenario.
        // 2. If resolver is not specified but a key is specified, match the key id on the key and and use it.

        Single<IKey> keySingle;

        if(this.keyResolver != null) {
            keySingle = Single.fromFuture(this.keyResolver.resolveKeyAsync(encryptionData.wrappedContentKey().keyId()))
                    .onErrorResumeNext(e -> {
                        if(e instanceof NullPointerException) {
                            return Single.error(new IllegalArgumentException(
                                    String.format("KeyResolver could not resolve encryption key: %s", encryptionData.wrappedContentKey().keyId())));
                        }
                        else {
                            return Single.error(e);
                        }
                    });
        }
        else {
            if(encryptionData.wrappedContentKey().keyId().equals(this.keyWrapper.getKid())) {
                keySingle = Single.just(this.keyWrapper);
            }
            else {
                return Single.error(new IllegalArgumentException(
                        "Key mismatch. The key id stored on the service does not match the specified encryption KeyID."));
            }
        }

        return keySingle.flatMap(keyEncryptionKey ->
                Single.fromFuture(keyEncryptionKey.unwrapKeyAsync(
                        encryptionData.wrappedContentKey().encryptedKey(),
                        encryptionData.wrappedContentKey().algorithm()
                )));
    }
}
