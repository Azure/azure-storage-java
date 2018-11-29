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

import com.microsoft.azure.storage.queue.MessagesURL;
import com.microsoft.azure.storage.queue.QueueURL;
import com.microsoft.azure.storage.queue.models.*;
import com.microsoft.rest.v2.Context;
import io.reactivex.Single;
import io.reactivex.Observable;

import java.net.MalformedURLException;
import java.security.InvalidKeyException;
import java.util.ArrayList;

/**
 * Represents a URL to the encrypted messages in the queue. This class is designed to mimic the interface of
 * {@link MessagesURL}.  Please refer to the
 * <a href=https://docs.microsoft.com/en-us/azure/storage/common/storage-client-side-encryption-java>Azure Docs</a>
 * for more information.
 */
public final class EncryptedMessagesURL {

    /**
     * Underlying {@link MessagesURL} to enqueue, peek, and dequeue messages.
     */
    private final MessagesURL messagesURL;

    /**
     * {@link MessageEncryptionPolicy} to encrypt and decrypt messages.
     */
    private final MessageEncryptionPolicy messageEncryptionPolicy;

    /**
     * Creates a new EncryptedMessagesURL with given MessagesUrl and MessageEncryptionPolicy.
     *
     * @param messagesURL A {@link MessagesURL}
     * @param messageEncryptionPolicy A {@link MessageEncryptionPolicy}
     */
    public EncryptedMessagesURL(MessagesURL messagesURL, MessageEncryptionPolicy messageEncryptionPolicy) {
        this.messagesURL = messagesURL;
        this.messageEncryptionPolicy = messageEncryptionPolicy;
    }

    /**
     * Creates a new EncryptedMessagesURL from a new MessagesURL created from given QueueURL and
     * MessageEncryptionPolicy.
     *
     * @param queueURL A {@link QueueURL}
     * @param messageEncryptionPolicy A {@link MessageEncryptionPolicy}
     */
    public EncryptedMessagesURL(QueueURL queueURL, MessageEncryptionPolicy messageEncryptionPolicy) {
        this.messagesURL = queueURL.createMessagesUrl();
        this.messageEncryptionPolicy = messageEncryptionPolicy;
    }

    /**
     * Creates a new EncryptedMessageIdURL using a MessageIdURL constructed by this EncryptedMessageURL's
     * MessageURL and MessageEncryptionPolicy
     *
     * @param messageId
     *          The messageID as a string.
     *
     * @return A {@link EncryptedMessageIdURL}
     */
    public EncryptedMessageIdURL createEncryptedMessageIdUrl(String messageId) {
        return new EncryptedMessageIdURL(this.messagesURL.createMessageIdUrl(messageId), this.messageEncryptionPolicy);
    }

    /**
     * Deletes all messages from a queue. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/clear-messages">Azure Docs</a>.
     *
     * @return Emits the successful response.
     */
    public Single<MessageClearResponse> clear() { return this.clear(null); }

    /**
     * Deletes all messages from a queue. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/clear-messages">Azure Docs</a>.
     *
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to
     *         its parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<MessageClearResponse> clear(Context context) { return this.messagesURL.clear(context); }

    /**
     * Adds a new encrypted message to the back of a queue.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/put-message">Azure Docs</a>.
     *
     * @param message
     *         The message content string that is up to 64KB in size.
     *
     * @return Emits the successful response.
     */
    public Single<MessageEnqueueResponse> enqueue(String message) throws InvalidKeyException {
        return this.enqueue(message, null, null, null);
    }

    /**
     * Adds a new encrypted message to the back of a queue.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/put-message">Azure Docs</a>.
     *
     * @param message
     *         The message content string that is up to 64KB in size.
     * @param messageTimeToLiveInSeconds
     *         Specifies the time-to-live interval for the message, in seconds.
     * @param visibilityTimeoutInSeconds
     *         The visibility timeout specifies how long the message should be invisible to Dequeue and Peek operations,
     *         in seconds.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to
     *         its parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<MessageEnqueueResponse> enqueue(String message, Integer messageTimeToLiveInSeconds,
            Integer visibilityTimeoutInSeconds, Context context) throws InvalidKeyException {
        return this.messageEncryptionPolicy.encryptMessage(message)
                .flatMap(encryptedMessage ->
                        this.messagesURL.enqueue(
                                encryptedMessage, messageTimeToLiveInSeconds, visibilityTimeoutInSeconds, context));
    }

    /**
     * Retrieves one or more messages from the front of the queue.
     * Note that messages may not be received in the order they were enqueued.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-messages">Azure Docs</a>.
     * Note that messages may not be dequeued in the order they were enqueued.
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
        return this.dequeue(numberOfMessages, visibilityTimeoutInSeconds, true, null);
    }

    /**
     * Retrieves one or more messages from the front of the queue.
     * Note that messages may not be received in the order they were enqueued.
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
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to
     *         its parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<MessageDequeueResponse> dequeue(
            int numberOfMessages, int visibilityTimeoutInSeconds, boolean requireEncryption, Context context) {
        return this.messagesURL.dequeue(numberOfMessages, visibilityTimeoutInSeconds, context).flatMap(response ->
                Observable.fromIterable(response.body())
                        .flatMapSingle(peekedMessageItem ->
                                this.messageEncryptionPolicy.decryptMessage(
                                        peekedMessageItem.messageText(), requireEncryption)
                                        .map(decryptedMessage ->
                                                peekedMessageItem.withMessageText(decryptedMessage)))
                        .collectInto(new ArrayList<DequeuedMessageItem>(), ArrayList::add)
                        .map(messageList ->
                                new MessageDequeueResponse(response.request(), response.statusCode(),
                                        response.headers(), response.rawHeaders(), messageList))
        );
    }

    /**
     * Retrieves one or more messages from the front of the queue but does not alter the visibility of the message.
     * Note that messages may not be received in the order they were enqueued.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/peek-messages">Azure Docs</a>.
     *
     * @param numberOfMessages
     *         A nonzero integer value that specifies the number of messages to peek from the queue, up to a maximum of
     *         32.  By default, a single message is peeked from the queue with this operation.
     *
     * @return Emits the successful response.
     */
    public Single<MessagePeekResponse> peek(int numberOfMessages) {
        return this.peek(numberOfMessages, true, null);
    }

    /**
     * Retrieves one or more messages from the front of the queue but does not alter the visibility of the message.
     * Note that messages may not be received in the order they were enqueued.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/peek-messages">Azure Docs</a>.
     *
     * @param numberOfMessages
     *         A nonzero integer value that specifies the number of messages to peek from the queue, up to a maximum of
     *         32. By default, a single message is peeked from the queue with this operation.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to
     *         its parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<MessagePeekResponse> peek(int numberOfMessages, boolean requireEncryption, Context context) {
        return this.messagesURL.peek(numberOfMessages, context).flatMap(response ->
                Observable.fromIterable(response.body())
                        .flatMapSingle(dequeuedMessageItem ->
                                this.messageEncryptionPolicy.decryptMessage(
                                        dequeuedMessageItem.messageText(), requireEncryption)
                                        .map(decryptedMessage ->
                                                dequeuedMessageItem.withMessageText(decryptedMessage)))
                        .collectInto(new ArrayList<PeekedMessageItem>(), ArrayList::add)
                        .map(messageList ->
                                new MessagePeekResponse(response.request(), response.statusCode(), response.headers(),
                                        response.rawHeaders(), messageList))
        );
    }
}
