package com.microsoft.azure.storage

import com.microsoft.azure.keyvault.cryptography.SymmetricKey
import com.microsoft.azure.storage.queue.MessagesURL
import com.microsoft.azure.storage.queue.QueueURL
import com.microsoft.azure.storage.queue.encryption.EncryptedMessageIdURL
import com.microsoft.azure.storage.queue.encryption.EncryptedMessagesURL
import com.microsoft.azure.storage.queue.encryption.MessageEncryptionPolicy
import com.microsoft.azure.storage.queue.models.EnqueuedMessage
import com.microsoft.azure.storage.queue.models.MessageEnqueueResponse
import com.microsoft.azure.storage.queue.models.MessageIDDeleteResponse
import com.microsoft.azure.storage.queue.models.MessageIDUpdateResponse
import com.microsoft.azure.storage.queue.models.MessagePeekResponse

class EncryptedMessageIdAPITest extends APISpec {
    QueueURL qu
    MessagesURL mu
    EncryptedMessagesURL emu
    EncryptedMessageIdURL emidu
    MessageEncryptionPolicy mep
    SymmetricKey key
    String messageText
    EnqueuedMessage message

    def setup() {
        qu = primaryServiceURL.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()
        mu = qu.createMessagesUrl()
        key = getSymmetricKey()
        mep = new MessageEncryptionPolicy(key, null)
        emu = new EncryptedMessagesURL(mu, mep)
        messageText = generateMessageText(100, entityNo++)

        MessageEnqueueResponse enqueueResponse = emu.enqueue(messageText).blockingGet()
        message = enqueueResponse.body().get(0)
        emidu = emu.createEncryptedMessageIdUrl(message.messageId())
    }

    // Deletes an encrypted message and then attempts to peek
    def "Delete and Peek Message"() {
        when:
        MessageIDDeleteResponse deleteResponse = emidu.delete(message.popReceipt()).blockingGet()
        MessagePeekResponse peekResponse = emu.peek(10).blockingGet()

        then:
        deleteResponse.statusCode() == 204

        peekResponse.statusCode() == 200
        peekResponse.body().size() == 0
    }

    // Updates encrypted message and then peeks it
    def "Update and Peek Message"() {
        setup:
        String newMessageText = generateMessageText(100, entityNo++)

        when:
        MessageIDUpdateResponse updateResponse = emidu.update(message.popReceipt(), 0, newMessageText).blockingGet()
        MessagePeekResponse peekResponse = emu.peek(10).blockingGet()

        then:
        updateResponse.statusCode() == 204

        peekResponse.statusCode() == 200
        peekResponse.body().get(0).messageText() == newMessageText
    }

    // Updated message, an peeks it with unencrypted MessagesURL
    def "Update and Peek Message Unencrypted"() {
        setup:
        String newMessageText = generateMessageText(100, entityNo++)

        when:
        MessageIDUpdateResponse updateResponse = emidu.update(message.popReceipt(), 0, newMessageText).blockingGet()
        MessagePeekResponse peekResponse = mu.peek(10).blockingGet()

        then:
        updateResponse.statusCode() == 204

        peekResponse.statusCode() == 200
        peekResponse.body().get(0).messageText() != newMessageText
    }

    // Updates encrypted message and then deletes it
    def "Update and Delete Message"() {
        setup:
        String newMessageText = generateMessageText(100, entityNo++)

        when:
        MessageIDUpdateResponse updateResponse = emidu.update(message.popReceipt(), 0, newMessageText).blockingGet()
        MessageIDDeleteResponse deleteResponse = emidu.delete(updateResponse.headers().popReceipt()).blockingGet()

        then:
        updateResponse.statusCode() == 204

        deleteResponse.statusCode() == 204
    }
}
