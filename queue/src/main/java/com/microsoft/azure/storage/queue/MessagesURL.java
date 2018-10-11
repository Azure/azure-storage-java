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
 * Represents a URL to the messages in the queue. It may be obtained by direct construction or via the
 * createMessagesUrl method on a {@link QueueURL} object. This class does not hold any state about a particular
 * message but is instead a convenient way of sending off appropriate requests to the resource on the service.
 * Please refer to the <a href=https://docs.microsoft.com/en-us/rest/api/storageservices/operations-on-messages>Azure Docs</a>
 * for more information.
 */
public final class MessagesURL extends StorageURL {

    public static final Integer MAX_NUMBER_OF_MESSAGES_TO_DEQUEUE = 32;

    /**
     * Creates a new {@link MessagesURL} object.
     *
     * @param url
     *         A {@code java.net.URL} to the messages in queue.
     * @param pipeline
     *         An {@link HttpPipeline} for sending requests.
     */
    public MessagesURL(URL url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link MessagesURL} with the given pipeline.
     *
     * @param pipeline
     *         An {@link HttpPipeline} object to set.
     *
     * @return A {@link MessagesURL} object with the given pipeline.
     */
    public MessagesURL withPipeline(HttpPipeline pipeline) {
        try {
            return new MessagesURL(new URL(this.storageClient.url()), pipeline);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new MessageIDURL object by concatenating messageID to the end of MessagesURL's URL.
     * The new MessageIDURL uses the same request policy pipeline as the MessagesURL.
     * To change the pipeline, create the MessageIDURL and then call its WithPipeline method passing in the
     * desired pipeline object. For more information, see the
     *
     * @param messageId
     *         The messageID as a string.
     *
     * @return A {@link MessageIdURL} object.
     */
    public MessageIdURL createMessageIdUrl(String messageId) {
        try {
            return new MessageIdURL(StorageURL.appendToURLPath(new URL(this.storageClient.url()), messageId),
                    this.storageClient.httpPipeline());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes all messages from a queue. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/clear-messages">Azure Docs</a>.
     *
     * @return Emits the successful response.
     */
    public Single<MessageClearResponse> clear() {
        return this.clear(null);
    }

    /**
     * Deletes all messages from a queue. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/clear-messages">Azure Docs</a>.
     *
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<MessageClearResponse> clear(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedMessages().
                clearWithRestResponseAsync(context, null, null));
    }

    /**
     * Adds a new message to the back of a queue.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/put-message">Azure Docs</a>.
     *
     * @param message
     *         The message content string that is up to 64KB in size.
     *
     * @return Emits the successful response.
     */
    public Single<MessageEnqueueResponse> enqueue(String message) {
        return this.enqueue(message, null, null, null);
    }

    /**
     * Adds a new message to the back of a queue.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/put-message">Azure Docs</a>.
     *
     * @param message
     *         The message content string that is up to 64KB in size.
     * @param messageTimeToLiveInSeconds
     *         Specifies the time-to-live interval for the message, in seconds.
     * @param visibilityTimeoutInSeconds
     *         The visibility timeout specifies how long the message should be invisible to Dequeue and Peek operations, in seconds.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<MessageEnqueueResponse> enqueue(String message, Integer messageTimeToLiveInSeconds, Integer visibilityTimeoutInSeconds, Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedMessages().
                enqueueWithRestResponseAsync(context, new QueueMessage().withMessageText(message), visibilityTimeoutInSeconds,
                        messageTimeToLiveInSeconds, null, null));
    }

    /**
     * Retrieves one or more messages from the front of the queue.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-messages">Azure Docs</a>.
     *
     * @param numberOfMessages
     *         A nonzero integer value that specifies the number of messages to retrieve from the queue, up to
     *         a maximum of 32. If fewer are visible, the visible messages are returned. By default, a single
     *         message is retrieved from the queue with this operation.
     * @param visibilityTimeoutInSeconds
     *         Specifies the new visibility timeout value, in seconds, relative to server time.
     *
     * @return Emits the successful response.
     */
    public Single<MessageDequeueResponse> dequeue(int numberOfMessages, int visibilityTimeoutInSeconds) {
        return this.dequeue(numberOfMessages, visibilityTimeoutInSeconds, null);
    }

    /**
     * Retrieves one or more messages from the front of the queue.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-messages">Azure Docs</a>.
     *
     * @param numberOfMessages
     *         A nonzero integer value that specifies the number of messages to retrieve from the queue, up to
     *         a maximum of 32. If fewer are visible, the visible messages are returned. By default, a single
     *         message is retrieved from the queue with this operation.
     * @param visibilityTimeoutInSeconds
     *         Specifies the new visibility timeout value, in seconds, relative to server time.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<MessageDequeueResponse> dequeue(int numberOfMessages, int visibilityTimeoutInSeconds, Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedMessages().
                dequeueWithRestResponseAsync(context, numberOfMessages, visibilityTimeoutInSeconds, null, null));
    }

    /**
     * Retrieves one or more messages from the front of the queue but does not alter the visibility of the message.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/peek-messages">Azure Docs</a>.
     *
     * @param numberOfMessages
     *         A nonzero integer value that specifies the number of messages to peek from the queue, up to a maximum of 32.
     *         By default, a single message is peeked from the queue with this operation.
     *
     * @return Emits the successful response.
     */
    public Single<MessagePeekResponse> peek(int numberOfMessages) {
        return this.peek(numberOfMessages, null);
    }

    /**
     * Retrieves one or more messages from the front of the queue but does not alter the visibility of the message.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/peek-messages">Azure Docs</a>.
     *
     * @param numberOfMessages
     *         A nonzero integer value that specifies the number of messages to peek from the queue, up to a maximum of 32.
     *         By default, a single message is peeked from the queue with this operation.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<MessagePeekResponse> peek(int numberOfMessages, Context context) {
        return addErrorWrappingToSingle(this.storageClient.generatedMessages().
                peekWithRestResponseAsync(context, numberOfMessages, null, null));
    }
}