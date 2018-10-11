package com.microsoft.azure.storage


import com.microsoft.azure.storage.queue.MessagesURL;
import com.microsoft.azure.storage.queue.QueueURL
import com.microsoft.azure.storage.queue.StorageException;
import com.microsoft.azure.storage.queue.models.DequeuedMessageItem
import com.microsoft.azure.storage.queue.models.MessageClearHeaders
import com.microsoft.azure.storage.queue.models.MessageClearResponse
import com.microsoft.azure.storage.queue.models.MessageDequeueHeaders
import com.microsoft.azure.storage.queue.models.MessageDequeueResponse
import com.microsoft.azure.storage.queue.models.MessageEnqueueHeaders
import com.microsoft.azure.storage.queue.models.MessageEnqueueResponse
import com.microsoft.azure.storage.queue.models.MessagePeekHeaders
import com.microsoft.azure.storage.queue.models.MessagePeekResponse
import com.microsoft.azure.storage.queue.models.PeekedMessageItem
import com.microsoft.rest.v2.http.HttpPipeline

class MessagesAPITest extends APISpec {
    QueueURL qu
    MessagesURL mu

    def setup() {
        qu = primaryServiceURL.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()
        mu = qu.createMessagesUrl()
    }

    def "Enqueue Message"() {
        when:
        MessageEnqueueResponse response = mu.enqueue(generateMessageText(), null, null, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 201
        response.body().size() == 1
    }

    def "Enqueue Message visibility timeout and time to live"() {
        when:
        MessageEnqueueResponse response = mu.enqueue(generateMessageText(), timeToLive, visibilityTimeOut, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 201
        response.body().size() == 1
        response.body()[0]

        where:
        visibilityTimeOut | timeToLive
        null              | null
        5                 | null
        null              | 5
        5                 | 15
    }

    def "Enqueue Message visibility timeout test "() {
        mu.enqueue(generateMessageText(), null, 10, null).blockingGet()

        when:
        MessagePeekResponse peekResponse1 = mu.peek(1, null).blockingGet()
        Thread.sleep(10000)
        MessagePeekResponse peekResponse2 = mu.peek(1, null).blockingGet()

        then:
        validateBasicHeaders(peekResponse1.headers())
        validateBasicHeaders(peekResponse2.headers())
        peekResponse1.statusCode() == 200
        peekResponse2.statusCode() == 200
        peekResponse1.body().size() == 0
        peekResponse2.body().size() == 1
    }

    def "Enqueue Message time to live test "() {
        setup:
        mu.enqueue(generateMessageText(), 10, null, null).blockingGet()

        when:
        MessagePeekResponse peekResponse1 = mu.peek(1, null).blockingGet()
        Thread.sleep(10000)
        MessagePeekResponse peekResponse2 = mu.peek(1, null).blockingGet()

        then:
        validateBasicHeaders(peekResponse1.headers())
        validateBasicHeaders(peekResponse2.headers())
        peekResponse1.statusCode() == 200
        peekResponse2.statusCode() == 200
        peekResponse1.body().size() == 1
        peekResponse2.body().size() == 0
    }

    def "Enqueue Visibility Timeout"() {
        mu.enqueue(generateMessageText(), null, 5, null).blockingGet()

        when:
        sleep(time * 1000)
        MessageDequeueResponse response = mu.dequeue(1, 1, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 200
        response.body().size() == value

        where:
        time | value
        0    | 0
        6    | 1
    }

    def "Enqueue min"() {
        expect:
        mu.enqueue(generateMessageText(), null, null, null).blockingGet().statusCode() == 201
    }

    def "Enqueue Message Error"() {
        when:
        mu.enqueue(null, null, null, null).blockingGet()

        then:
        thrown(RuntimeException)
    }

    def "Enqueue context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(201, MessageEnqueueHeaders)))

        def mu = primaryServiceURL.withPipeline(pipeline).createQueueUrl(generateQueueName()).createMessagesUrl()

        when:
        // No service call is made. Just satisfy the parameters.
        mu.enqueue(generateMessageText(), null, null, defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Dequeue Message"() {
        setup:
        mu.enqueue(generateMessageText(), null, null, null).blockingGet()

        when:
        MessageDequeueResponse response = mu.dequeue(1, 1, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 200
        response.body().size() == 1
        DequeuedMessageItem dqItem = response.body()[0]
        dqItem.messageId() != null
        dqItem.popReceipt() != null
        dqItem.insertionTime() != null
        dqItem.expirationTime() != null
        dqItem.dequeueCount() == 1
    }

    def "Dequeue Number of Messages"() {
        setup:
        for (int i = 0; i < 10; i++)
            mu.enqueue(generateMessageText(), null, null, null).blockingGet()

        when:
        MessageDequeueResponse response = mu.dequeue(10, 1, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 200
        response.body().size() == 10
        for (int i = 0; i < 10; i++) {
            DequeuedMessageItem dqItem = response.body()[i]
            assert dqItem.messageId() != null
            assert dqItem.popReceipt() != null
            assert dqItem.insertionTime() != null
            assert dqItem.expirationTime() != null
            assert dqItem.dequeueCount() == 1
        }
    }

    def "Dequeue min"() {
        expect:
        mu.dequeue(1, 1).blockingGet().statusCode() == 200
    }

    def "Dequeue Error"() {
        setup:
        for (int i = 0; i < 10; i++)
            mu.enqueue(generateMessageText(), null, null, null).blockingGet()

        when:
        mu.dequeue(100, 1, null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Dequeue context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(200, MessageDequeueHeaders)))

        def mu = primaryServiceURL.withPipeline(pipeline).createQueueUrl(generateQueueName()).createMessagesUrl()

        when:
        // No service call is made. Just satisfy the parameters.
        mu.dequeue(1, 0, defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Peek empty queue"() {
        when:
        MessagePeekResponse response = mu.peek(10, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 200
        response.body().size() == 0
    }

    def "Peek Messages"() {
        setup:
        mu.enqueue(generateMessageText(), null, null, null).blockingGet()

        when:
        MessagePeekResponse response = mu.peek(1, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 200
        response.body().size() == 1
        PeekedMessageItem pkItem = response.body()[0]
        pkItem.messageId() != null
        pkItem.insertionTime() != null
        pkItem.expirationTime() != null
        pkItem.dequeueCount() == 0
        pkItem.messageText().startsWith(queueMsgPrefix)
    }

    def "Peek min"() {
        expect:
        mu.peek(1).blockingGet().statusCode() == 200
    }

    def "Peek Error"() {
        setup:
        for (int i = 0; i < 10; i++)
            mu.enqueue(generateMessageText(), null, null, null).blockingGet()

        when:
        mu.peek(100, null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Peek Number of Messages"() {
        setup:
        qu = primaryServiceURL.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()
        MessagesURL mu = qu.createMessagesUrl()
        for (int i = 0; i < 10; i++)
            mu.enqueue(generateMessageText(), null, null, null).blockingGet()

        when:
        MessagePeekResponse response = mu.peek(10, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 200
        response.body().size() == 10
        for (PeekedMessageItem pkItem : response.body()) {
            assert pkItem.messageId() != null
            assert pkItem.insertionTime() != null
            assert pkItem.expirationTime() != null
            assert pkItem.dequeueCount() == 0
            assert pkItem.messageText().startsWith(queueMsgPrefix)
        }
    }

    def "Peek context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(200, MessagePeekHeaders)))

        def mu = primaryServiceURL.withPipeline(pipeline).createQueueUrl(generateQueueName()).createMessagesUrl()

        when:
        // No service call is made. Just satisfy the parameters.
        mu.peek(1, defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Clear Queue"() {
        setup:
        for (int i = 0; i < 10; i++)
            mu.enqueue(generateMessageText(), null, null, null).blockingGet()


        when:
        MessageClearResponse clearResponse = mu.clear().blockingGet()
        MessagePeekResponse peekResponse = mu.peek(10, null).blockingGet()

        then:
        validateBasicHeaders(clearResponse.headers())
        clearResponse.statusCode() == 204
        peekResponse.statusCode() == 200
        peekResponse.body().size() == 0
    }

    def "Clear min"() {
        expect:
        mu.clear().blockingGet().statusCode() == 204
    }

    def "Clear context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(204, MessageClearHeaders)))

        def mu = primaryServiceURL.withPipeline(pipeline).createQueueUrl(generateQueueName()).createMessagesUrl()

        when:
        // No service call is made. Just satisfy the parameters.
        mu.clear(defaultContext)

        then:
        notThrown(RuntimeException)
    }
}
