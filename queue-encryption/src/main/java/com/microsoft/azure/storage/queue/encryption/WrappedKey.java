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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * RESERVED FOR INTERNAL USE. Represents the envelope key details stored on the service.
 */
final class WrappedKey {

    /**
     * The key identifier string.
     */
    @JsonProperty(value = "KeyId", required = true)
    private String keyId;

    /**
     * The encrypted content encryption key.
     */
    @JsonProperty(value = "EncryptedKey", required = true)
    private byte[] encryptedKey;

    /**
     * The algorithm used for wrapping.
     */
    @JsonProperty(value = "Algorithm", required = true)
    private String algorithm;

    /**
     * Initializes a new instance of the {@link WrappedKey} class.
     */
    public WrappedKey() {}

    /**
     * Initializes a new instance of the {@link WrappedKey} class using the specified key id, encrypted key and
     * the algorithm.
     *
     * @param keyId
     *              The key identifier string.
     * @param encryptedKey
     *              The encrypted content encryption key.
     * @param algorithm
     *              The algorithm used for wrapping.
     */
    public WrappedKey(String keyId, byte[] encryptedKey, String algorithm) {
        this.keyId = keyId;
        this.encryptedKey = encryptedKey;
        this.algorithm = algorithm;
    }

    /**
     * Gets the key identifier. This identifier is used to identify the key that is used to wrap/unwrap the content
     * encryption key.
     *
     * @return The key identifier string.
     */
    public String keyId() {
        return keyId;
    }

    /**
     * Gets the encrypted content encryption key.
     *
     * @return The encrypted content encryption key.
     */
    public byte[] encryptedKey() {
        return encryptedKey;
    }

    /**
     * Gets the algorithm used for wrapping.
     *
     * @return The algorithm used for wrapping.
     */
    public String algorithm() {
        return algorithm;
    }

    /**
     * Sets the key identifier. This identifier is used to identify the key that is used to wrap/unwrap the content
     * encryption key.
     *
     * @param keyId
     *              The key identifier string.
     *
     * @return this
     */
    public WrappedKey withKeyId(String keyId) {
        this.keyId = keyId;
        return this;
    }

    /**
     * Sets the encrypted content encryption key.
     *
     * @param encryptedKey
     *              The encrypted content encryption key.
     *
     * @return this
     */
    public WrappedKey withEncryptedKey(byte[] encryptedKey) {
        this.encryptedKey = encryptedKey;
        return this;
    }

    /**
     * Sets the algorithm used for wrapping.
     *
     * @param algorithm
     *              The algorithm used for wrapping.
     *
     * @return this
     */
    public WrappedKey withAlgorithm(String algorithm) {
        this.algorithm = algorithm;
        return this;
    }
}
