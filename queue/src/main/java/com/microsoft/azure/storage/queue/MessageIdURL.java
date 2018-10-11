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
package com.microsoft.azure.storage.queue;

import com.microsoft.azure.storage.queue.models.*;
import com.microsoft.rest.v2.Context;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Single;

import java.net.MalformedURLException;
import java.net.URL;

import static com.microsoft.azure.storage.queue.Utility.addErrorWrappingToSingle;

/**
 * Represents a URL to a specific message in the queue. It may be obtained by direct construction or via the
 * createMessageIdUrl method on a {@link MessagesURL} object. This class does hold the state about a specific message.
 * Please refer to the
 * <a href=https://docs.microsoft.com/en-us/rest/api/storageservices/operations-on-messages>Azure Docs</a>
 * for more information.
 */
public final class MessageIdURL extends StorageURL {

    /**
     * Creates a new {@link MessageIdURL} object.
     *
     * @param url
     *         A {@code java.net.URL} to a message.
     * @param pipeline
     *         An {@link HttpPipeline} for sending requests.
     */
    public MessageIdURL(URL url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link MessageIdURL} with the given pipeline.
     *
     * @param pipeline
     *         An {@link HttpPipeline} object to set.
     *
     * @return A {@link MessageIdURL} object with the given pipeline.
     */
    public MessageIdURL withPipeline(HttpPipeline pipeline) {
        try {
            return new MessageIdURL(new URL(this.storageClient.url()), pipeline);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
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
        return this.delete(popReceipt, null);
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
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedMessageIDs().
                deleteWithRestResponseAsync(context, popReceipt, null, null));
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
     *         The new visibility timeout value, in seconds, relative to server time. The new value must be larger than or
     *         equal to 0, and cannot be larger than 7 days. The visibility timeout of a message cannot be set to a value later than
     *         the expiry time. A message can be updated until it has been deleted or has expired.
     * @param message
     *         Updated message content string.
     *
     * @return Emits the successful response.
     */
    public Single<MessageIDUpdateResponse> update(String popReceipt, int visibilityTimeoutInSeconds, String message) {
        return this.update(popReceipt, visibilityTimeoutInSeconds, message, null);
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
     *         The new visibility timeout value, in seconds, relative to server time. The new value must be larger than or
     *         equal to 0, and cannot be larger than 7 days. The visibility timeout of a message cannot be set to a value later than
     *         the expiry time. A message can be updated until it has been deleted or has expired.
     * @param message
     *         Updated queue message content string.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<MessageIDUpdateResponse> update(String popReceipt, int visibilityTimeoutInSeconds, String message, Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedMessageIDs().
                updateWithRestResponseAsync(context, new QueueMessage().withMessageText(message),
                        popReceipt, visibilityTimeoutInSeconds, null, null));
    }
}