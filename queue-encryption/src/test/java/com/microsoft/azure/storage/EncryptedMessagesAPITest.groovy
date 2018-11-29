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

package com.microsoft.azure.storage

import com.google.common.util.concurrent.SettableFuture
import com.microsoft.azure.keyvault.core.IKey
import com.microsoft.azure.keyvault.core.IKeyResolver
import com.microsoft.azure.keyvault.cryptography.SymmetricKey
import com.microsoft.azure.storage.queue.MessagesURL
import com.microsoft.azure.storage.queue.QueueURL
import com.microsoft.azure.storage.queue.encryption.Constants
import com.microsoft.azure.storage.queue.encryption.EncryptedMessagesURL
import com.microsoft.azure.storage.queue.encryption.MessageEncryptionPolicy
import com.microsoft.azure.storage.queue.models.MessageDequeueResponse
import com.microsoft.azure.storage.queue.models.MessageEnqueueResponse
import com.microsoft.azure.storage.queue.models.MessagePeekResponse
import com.microsoft.rest.v2.Context
import spock.lang.Unroll

import javax.crypto.KeyGenerator

class EncryptedMessagesAPITest extends APISpec {
    static SymmetricKey key
    static SymmetricKey invalidKey
    static String message
    QueueURL qu
    MessagesURL mu
    EncryptedMessagesURL emu
    SymmetricKey invalidKeySameKeyId
    MessageEncryptionPolicy mep

    def setup() {
        qu = primaryServiceURL.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()
        mu = qu.createMessagesUrl()

        message = generateMessageText(100, entityNo++)

        key = getSymmetricKey()

        KeyGenerator keyGen = KeyGenerator.getInstance("AES")
        keyGen.init(256)
        invalidKeySameKeyId = new SymmetricKey(keyId, keyGen.generateKey().getEncoded())
        invalidKey = new SymmetricKey("invalidId", keyGen.generateKey().getEncoded())

        mep = new MessageEncryptionPolicy(key, null)
        emu = new EncryptedMessagesURL(mu, mep)
    }

    // Enqueue an encrypted message
    def "Enqueue message"() {
        when:
        MessageEnqueueResponse response = emu.enqueue(message, null, null, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 201
        response.body().size() == 1
    }

    // Enqueue and dequeue a encrypted message with non-English characters.  This tests for unicode characters > 1 byte long.
    def "Enqueue dequeue non-English message"() {
        setup:
        String m = "你好，今天好吗？我很好。"

        when:
        MessageEnqueueResponse enqueueResponse = emu.enqueue(m, null, null, null).blockingGet()
        MessageDequeueResponse dequeueResponse = emu.dequeue(1, 10).blockingGet()

        then:
        validateBasicHeaders(enqueueResponse.headers())
        enqueueResponse.statusCode() == 201
        enqueueResponse.body().size() == 1

        validateBasicHeaders(dequeueResponse.headers())
        dequeueResponse.statusCode() == 200
        dequeueResponse.body().size() == 1
        dequeueResponse.body().get(0).messageText() == m
    }

    // Encrypts and enqueues a message, dequeues without decryption
    def "Dequeue message without decryption"() {
        setup:
        emu.enqueue(message, null, null, null).blockingGet()

        when:
        // Note we are using a regular messagesURL to dequeue the message
        MessageDequeueResponse dequeueResponse = mu.dequeue(1, 10).blockingGet()

        then:
        validateBasicHeaders(dequeueResponse.headers())
        dequeueResponse.statusCode() == 200
        dequeueResponse.body().size() == 1
        message != dequeueResponse.body().get(0).messageText()
    }

    // Attempt to enqueue a null message
    def "Enqueue null message"() {
        when:
        emu.enqueue(null, null, null, null).blockingGet()

        then:
        IllegalArgumentException e = thrown(RuntimeException)
        e.message == "The argument must not be null or an empty string. Argument name: message."
    }

    // Attempt to enqueue a message that exceeds maximum message size
    def "Enqueue larger than max message"() {
        when:
        String m = generateMessageText(Constants.MAX_MESSAGE_SIZE + 5, entityNo++)
        emu.enqueue(m, null, null, null).blockingGet()

        then:
        IllegalArgumentException e = thrown(RuntimeException)
        e.message == "Encrypted message exceeds maximum queue message size"
    }

    // Attempt to enqueue a message using a null key encryption key
    def "Enqueue null key"() {
        when:
        MessageEncryptionPolicy messageEncryptionPolicy = new MessageEncryptionPolicy(null, Mock(IKeyResolver))
        EncryptedMessagesURL encryptedMessagesURL = new EncryptedMessagesURL(mu, messageEncryptionPolicy)
        encryptedMessagesURL.enqueue(null, null, null, null).blockingGet()

        then:
        IllegalArgumentException e = thrown(RuntimeException)
        e.message == "The argument must not be null or an empty string. Argument name: key."
    }

    // Dequeue an encrypted message
    def "Dequeue message"() {
        when:
        emu.enqueue(message, null, null, null).blockingGet()
        MessageDequeueResponse dequeueResponse = emu.dequeue(1, 10).blockingGet()

        then:
        validateBasicHeaders(dequeueResponse.headers())
        dequeueResponse.statusCode() == 200
        dequeueResponse.body().size() == 1
        message == dequeueResponse.body().get(0).messageText()
    }

    // Dequeue multiple encrypted messages
    def "Dequeue multiple messages"() {
        setup:
        int messageCount = 3
        String[] messages = new String[messageCount]
        for(int i = 0; i < messageCount; i++) {
            messages[i] = generateMessageText(100, entityNo++)
            emu.enqueue(messages[i], null, null, null).blockingGet()
        }

        when:
        MessageDequeueResponse dequeueResponse = emu.dequeue(5, 10).blockingGet()

        then:
        validateBasicHeaders(dequeueResponse.headers())
        dequeueResponse.statusCode() == 200
        dequeueResponse.body().size() == messageCount
        for(int i = 0; i < messageCount; i++) {
            messages[i] == dequeueResponse.body().get(i).messageText()
        }
    }

    // Dequeue encrypted message using a MessageEncryptionPolicy with a KeyResolver and valid key, null key, and invalid key
    @Unroll
    def "Dequeue message KeyResolver tests"() {
        setup:
        IKeyResolver mockKeyResolver = getMockKeyResolver(key, true)
        MessageEncryptionPolicy messageEncryptionPolicy = new MessageEncryptionPolicy(key, mockKeyResolver)
        EncryptedMessagesURL encryptedMessagesURL = new EncryptedMessagesURL(mu, messageEncryptionPolicy)
        encryptedMessagesURL.enqueue(message, null, null, null).blockingGet()

        MessageEncryptionPolicy newMessageEncryptionPolicy = new MessageEncryptionPolicy(newKey, mockKeyResolver)
        EncryptedMessagesURL newEncryptedMessagesURL = new EncryptedMessagesURL(mu, newMessageEncryptionPolicy)

        when:
        MessageDequeueResponse dequeueResponse = newEncryptedMessagesURL.dequeue(1, 10).blockingGet()

        then:
        validateBasicHeaders(dequeueResponse.headers())
        dequeueResponse.statusCode() == 200
        dequeueResponse.body().size() == 1
        message == dequeueResponse.body().get(0).messageText()

        where:
        newKey << [key, null, invalidKey]
    }

    // Attempt to dequeue a message using a KeyResolver that cannot resolve specified key
    def "Dequeue message KeyResolver key not found"() {
        setup:
        IKeyResolver mockKeyResolver = getMockKeyResolver(key, false)
        MessageEncryptionPolicy messageEncryptionPolicy = new MessageEncryptionPolicy(key, mockKeyResolver)
        EncryptedMessagesURL encryptedMessagesURL = new EncryptedMessagesURL(mu, messageEncryptionPolicy)
        encryptedMessagesURL.enqueue(message, null, null, null).blockingGet()

        when:
        MessageDequeueResponse dequeueResponse = encryptedMessagesURL.dequeue(1, 10).blockingGet()

        then:
        IllegalArgumentException e = thrown(IllegalArgumentException)
        e.message == "KeyResolver could not resolve encryption key: keyId"
    }

    // Dequeue a non-encrypted message with requireEncryption = false, and on a message containing the String "EncryptionData"
    @Unroll
    def "Dequeue non-encrypted requireEncryption false"() {
        setup:
        mu.enqueue(message).blockingGet()

        when:
        MessageDequeueResponse dequeueResponse = emu.dequeue(1, 10, false, Context.NONE).blockingGet()

        then:
        validateBasicHeaders(dequeueResponse.headers())
        dequeueResponse.statusCode() == 200
        dequeueResponse.body().size() == 1
        message == dequeueResponse.body().get(0).messageText()

        where:
        m << [message, "EncryptionData"]
    }

    /* Attempt Dequeue a non-encrypted message with requireEncryption = true on a regular unencrypted message,
     * and an uncrypted message containing the String "EncryptionData"
     */
    @Unroll
    def "Dequeue non-encrypted EncryptionData true"() {
        setup:
        mu.enqueue(m).blockingGet()

        when:
        MessageDequeueResponse dequeueResponse = emu.dequeue(1, 10, true, Context.NONE).blockingGet()

        then:
        IllegalArgumentException e = thrown(IllegalArgumentException)
        e.message == "Encryption data does not exist. If you do not want to decrypt the data, please do not set the require encryption flag to true"

        where:
        m << ["EncryptionData", generateMessageText(100, entityNo++)]
    }

    // Attempt Dequeue a non-encrypted message with requireEncryption = false on a message containing the String EncryptionData that is valid json
    def "Dequeue non-encrypted EncryptionData json false"() {
        setup:
        mu.enqueue("{\"EncryptionData\": true}").blockingGet()

        when:
        MessageDequeueResponse dequeueResponse = emu.dequeue(1, 10, true, Context.NONE).blockingGet()

        then:
        RuntimeException e = thrown(RuntimeException)
    }

    // Attempt to dequeue an encrypted message when Key and KeyResolver are both null
    def "Dequeue with null Key and KeyResolver"() {
        setup:
        EncryptedMessagesURL encryptedMessagesURL = new EncryptedMessagesURL(mu, new MessageEncryptionPolicy(null, null))
        emu.enqueue(message, null, null, null).blockingGet()

        when:

        encryptedMessagesURL.dequeue(1, 10).blockingGet()

        then:
        IllegalArgumentException e = thrown(IllegalArgumentException)
        e.message == "Key and KeyResolver cannot both be null"
    }

    // Attempt to dequeue encrypted message with an invalid key encryption key
    def "Dequeue with invalid key"() {
        setup:
        MessageEncryptionPolicy policy = new MessageEncryptionPolicy(invalidKey, null)
        EncryptedMessagesURL url = new EncryptedMessagesURL(mu, policy)
        emu.enqueue(message, null, null, null).blockingGet()

        when:
        url.dequeue(1, 10).blockingGet()

        then:
        RuntimeException e = thrown(RuntimeException)
        e.message == "Key mismatch. The key id stored on the service does not match the specified encryption KeyID."
    }

    // Peek an encrypted message
    def "Peek message"() {
        setup:
        emu.enqueue(message, null, null, null).blockingGet()

        when:
        MessagePeekResponse peekResponse = emu.peek(1).blockingGet()

        then:
        validateBasicHeaders(peekResponse.headers())
        peekResponse.statusCode() == 200
        peekResponse.body().size() == 1
        message == peekResponse.body().get(0).messageText()
    }

    // Peek multiple encrypted messages
    def "Peek multiple messages"() {
        setup:
        int messageCount = 3
        String[] messages = new String[messageCount]
        for(int i = 0; i < messageCount; i++) {
            messages[i] = generateMessageText(100, entityNo++)
            emu.enqueue(messages[i], null, null, null).blockingGet()
        }

        when:
        MessagePeekResponse peekResponse = emu.peek(5).blockingGet()

        then:
        validateBasicHeaders(peekResponse.headers())
        peekResponse.statusCode() == 200
        peekResponse.body().size() == messageCount
        for(int i = 0; i < messageCount; i++) {
            messages[i] == peekResponse.body().get(i).messageText()
        }
    }

    def getMockKeyResolver(IKey key, boolean foundKey) {
        return Mock(IKeyResolver) {
            resolveKeyAsync(*_) >> { String s ->
                SettableFuture<IKey> result = new SettableFuture<>()
                if(foundKey) {
                    result.set(key)
                }
                else {
                    result.set(null)
                }
                return result
            }
        }
    }
}
