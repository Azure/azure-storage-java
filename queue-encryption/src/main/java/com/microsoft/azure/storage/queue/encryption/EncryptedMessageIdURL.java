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

import com.microsoft.azure.storage.queue.MessageIdURL;
import com.microsoft.azure.storage.queue.models.MessageIDDeleteResponse;
import com.microsoft.azure.storage.queue.models.MessageIDUpdateResponse;
import com.microsoft.rest.v2.Context;
import io.reactivex.Single;

import java.security.InvalidKeyException;

/**
 * Represents a URL to a specific encrypted message in the queue. It may be obtained by direct construction or via the
 * createEncrypted MessageIdUrl method on a {@link EncryptedMessagesURL} object. This class does hold the state
 * about a specific message.
 * Please refer to the
 * <a href=https://docs.microsoft.com/en-us/rest/api/storageservices/operations-on-messages>Azure Docs</a>
 * for more information.
 */
public final class EncryptedMessageIdURL {

    /**
     * Underlying {@link MessageIdURL} to delete and update messages.
     */
    private final MessageIdURL messageIdURL;

    /**
     * {@link MessageEncryptionPolicy} to encrypt and decrypt messages.
     */
    private final MessageEncryptionPolicy messageEncryptionPolicy;

    /**
     * Creates a new EncryptedMessageIdURL with given MessageIdURL and MessageEncryptionPolicy
     *
     * @param messageIdURL A {@link MessageIdURL}
     * @param messageEncryptionPolicy A {@link MessageEncryptionPolicy}
     */
    public EncryptedMessageIdURL(MessageIdURL messageIdURL, MessageEncryptionPolicy messageEncryptionPolicy) {
        this.messageIdURL = messageIdURL;
        this.messageEncryptionPolicy = messageEncryptionPolicy;
    }

    /**
     * Deletes the specified message. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/delete-message2">Azure Docs</a>.
     *
     * @param popReceipt
     *         A valid pop receipt value returned from an earlier call to the Get Messages or Update Message operation.
     *
     * @return Emits the successful response.
     */
    public Single<MessageIDDeleteResponse> delete(String popReceipt) {
        return this.messageIdURL.delete(popReceipt);
    }

    /**
     * Deletes the specified message. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/delete-message2">Azure Docs</a>.
     *
     * @param popReceipt
     *         A valid pop receipt value returned from an earlier call to the Get Messages or Update Message operation.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<MessageIDDeleteResponse> delete(String popReceipt, Context context) {
        return this.messageIdURL.delete(popReceipt, context);
    }

    /**
     * Updates the visibility timeout of a message. You can also use this operation to update the contents of a message.
     * A message must be in a format that can be included in an XML request with UTF-8 encoding, and the encoded message
     * can be up to 64KB in size. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/update-message">Azure Docs</a>.
     *
     * @param popReceipt
     *         The valid pop receipt value returned from an earlier call to the Get Messages or Update Message operation.
     * @param visibilityTimeoutInSeconds
     *         The new visibility timeout value, in seconds, relative to server time. The new value must be larger than
     *         or equal to 0, and cannot be larger than 7 days. The visibility timeout of a message cannot be set to a
     *         value later than the expiry time. A message can be updated until it has been deleted or has expired.
     * @param message
     *         Updated message content string.
     *
     * @return Emits the successful response.
     */
    public Single<MessageIDUpdateResponse> update(String popReceipt, int visibilityTimeoutInSeconds, String message)
            throws InvalidKeyException {
        return this.update(popReceipt, visibilityTimeoutInSeconds, message, Context.NONE);
    }

    /**
     * Updates the visibility timeout of an encrypted message. You can also use this operation to update the contents
     * of a message.  A message must be in a format that can be included in an XML request with UTF-8 encoding, and
     * the encoded message can be up to 64KB in size. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/update-message">Azure Docs</a>.
     *
     * @param popReceipt
     *         The valid pop receipt value returned from an earlier call to the Get Messages or Update Message operation.
     * @param visibilityTimeoutInSeconds
     *         The new visibility timeout value, in seconds, relative to server time. The new value must be larger than
     *         or equal to 0, and cannot be larger than 7 days. The visibility timeout of a message cannot be set to a
     *         value later than the expiry time. A message can be updated until it has been deleted or has expired.
     * @param message
     *         Updated queue message content string.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to
     *         its parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<MessageIDUpdateResponse> update(
            String popReceipt, int visibilityTimeoutInSeconds, String message, Context context) throws InvalidKeyException {
        return this.messageEncryptionPolicy.encryptMessage(message)
                .flatMap(encryptedMessage ->
                        this.messageIdURL.update(popReceipt, visibilityTimeoutInSeconds, encryptedMessage, context));
    }
}
