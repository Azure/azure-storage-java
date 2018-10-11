package com.microsoft.azure.storage

import com.microsoft.azure.storage.queue.MessageIdURL
import com.microsoft.azure.storage.queue.MessagesURL
import com.microsoft.azure.storage.queue.QueueURL
import com.microsoft.azure.storage.queue.StorageException
import com.microsoft.azure.storage.queue.models.EnqueuedMessage
import com.microsoft.azure.storage.queue.models.MessageIDDeleteHeaders
import com.microsoft.azure.storage.queue.models.MessageIDDeleteResponse
import com.microsoft.azure.storage.queue.models.MessageIDUpdateHeaders
import com.microsoft.azure.storage.queue.models.MessageIDUpdateResponse
import com.microsoft.azure.storage.queue.models.MessagePeekResponse
import com.microsoft.rest.v2.http.HttpPipeline

class MessageIDAPITest extends APISpec {
    QueueURL qu
    MessagesURL mu
    EnqueuedMessage message

    def setup() {
        qu = primaryServiceURL.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()
        mu = qu.createMessagesUrl()
        message = mu.enqueue(generateMessageText(), null, null, null).blockingGet().body()[0]
    }

    def "Delete Message"() {
        when:
        MessageIdURL mIdu = mu.createMessageIdUrl(message.messageId())
        MessageIDDeleteResponse response = mIdu.delete(message.popReceipt(), null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 204
    }

    def "Delete Message Error"() {
        when:
        MessageIdURL mIdu = mu.createMessageIdUrl(message.messageId().reverse())
        mIdu.delete(popReceipt, null).blockingGet()

        then:
        thrown(exception)

        where:
        popReceipt | exception
        "garbage"  | StorageException
        null       | IllegalArgumentException
    }

    def "Delete min"() {
        expect:
        mu.createMessageIdUrl(message.messageId()).delete(message.popReceipt()).blockingGet().statusCode() == 204
    }

    def "Delete and Peek Message"() {
        setup:
        mu = qu.createMessagesUrl()
        mu.clear(null).blockingGet()
        List<EnqueuedMessage> enueuedMessageList = new ArrayList<EnqueuedMessage>()
        for (int i = 0; i < 10; i++) {
            enueuedMessageList.add(mu.enqueue(generateMessageText(), null, null, null).blockingGet().body()[0])
        }

        when:
        List<MessageIDDeleteResponse> deleteResponse = new ArrayList<MessageIDDeleteResponse>()
        for (int i = 0; i < 5; i++) {
            MessageIdURL mIdu = mu.createMessageIdUrl(enueuedMessageList[i].messageId())
            deleteResponse.add(mIdu.delete(enueuedMessageList[i].popReceipt(), null).blockingGet())
        }
        MessagePeekResponse peekResponse = mu.peek(10, null).blockingGet()

        then:
        validateBasicHeaders(peekResponse.headers())
        for (MessageIDDeleteResponse response : deleteResponse) {
            assert response.statusCode() == 204
            assert validateBasicHeaders(response.headers())
        }
        peekResponse.statusCode() == 200
        peekResponse.body().size() == 5
    }

    def "Delete message context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(204, MessageIDDeleteHeaders)))

        MessageIdURL mIdu = mu.createMessageIdUrl(message.messageId()).withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        mIdu.delete(generateMessageText(), defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Update Message"() {
        when:
        MessageIdURL mIdu = mu.createMessageIdUrl(message.messageId())
        MessageIDUpdateResponse updateResponse = mIdu.update(message.popReceipt(), 1, generateMessageText(), null).blockingGet()

        then:
        validateBasicHeaders(updateResponse.headers())
        updateResponse.statusCode() == 204
        updateResponse.headers().popReceipt() != null
        updateResponse.headers().timeNextVisible() != null
    }

    def "Update and Peek Message"() {
        when:
        String updatedMessageText = generateMessageText()
        MessageIdURL mIdu = mu.createMessageIdUrl(message.messageId())
        MessageIDUpdateResponse updateResponse = mIdu.update(message.popReceipt(), 1, updatedMessageText, null).blockingGet()
        mu.peek(10, null).blockingGet().body()[0]

        then:
        validateBasicHeaders(updateResponse.headers())
        updateResponse.statusCode() == 204
        updateResponse.headers().popReceipt() != null
        updateResponse.headers().timeNextVisible() != null
    }

    def "Update Message Error"() {
        when:
        MessageIdURL mIdu = mu.createMessageIdUrl(message.messageId())
        mIdu.update(popReceipt, visibilityTimeout, queueMessage, null).blockingGet()

        then:
        thrown(RuntimeException)

        where:
        popReceipt | queueMessage | visibilityTimeout
        null       | "randomText" | 5
        "random"   | null         | 10
    }

    def "Update min"() {
        expect:
        mu.createMessageIdUrl(message.messageId()).update(message.popReceipt(), 10, generateMessageText())
                .blockingGet().statusCode() == 204
    }

    def "Update message context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(204, MessageIDUpdateHeaders)))

        MessageIdURL mIdu = mu.createMessageIdUrl(message.messageId()).withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        mIdu.update(generateMessageText(), 0, generateMessageText(), defaultContext)

        then:
        notThrown(RuntimeException)
    }
}
