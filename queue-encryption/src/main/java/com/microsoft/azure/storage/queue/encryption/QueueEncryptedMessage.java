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
 * Represents the encrypted message that is stored on the service.
 */
final class QueueEncryptedMessage {

    /**
     * The encrypted message.
     */
    @JsonProperty(value = "EncryptedMessageContents", required = true)
    private String encryptedMessageContents;

    /**
     * The encryption related metadata for queue messages.
     */
    @JsonProperty(value = "EncryptionData", required = true)
    private EncryptionData encryptionData;

    /**
     * Constructor
     */
    public QueueEncryptedMessage() {

    }

    /**
     *
     *
     * @param encryptedMessageContents
     *          encrypted message contents.
     * @param encryptionData
     *          encryption data.
     */
    public QueueEncryptedMessage(String encryptedMessageContents, EncryptionData encryptionData) {
        this.encryptedMessageContents = encryptedMessageContents;
        this.encryptionData = encryptionData;
    }

    /**
     * Gets the encrypted message contents.
     *
     * @return encrypted message contents.
     */
    public String encryptedMessageContents() {
        return this.encryptedMessageContents;
    }

    /**
     * Gets the encryption related metadata for queue messages.
     *
     * @return The encryption metadata.
     */
    public EncryptionData encryptionData() {
        return this.encryptionData;
    }

    /**
     * Sets the encrypted message contents.
     *
     * @param encryptedMessageContents
     *          encrypted message contents.
     *
     * @return this
     */
    public QueueEncryptedMessage withEncryptedMessageContents(String encryptedMessageContents) {
        this.encryptedMessageContents = encryptedMessageContents;
        return this;
    }

    /**
     * Sets the encryption data.
     *
     * @param encryptionData
     *          Encryption data.
     *
     * @return this
     */
    public QueueEncryptedMessage withEncryptionData(EncryptionData encryptionData) {
        this.encryptionData = encryptionData;
        return this;
    }
}
