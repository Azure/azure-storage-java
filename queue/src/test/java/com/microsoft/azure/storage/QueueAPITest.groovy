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

import com.microsoft.azure.storage.queue.Metadata
import com.microsoft.azure.storage.queue.QueueURL
import com.microsoft.azure.storage.queue.ServiceURL
import com.microsoft.azure.storage.queue.StorageException
import com.microsoft.azure.storage.queue.models.AccessPolicy
import com.microsoft.azure.storage.queue.models.QueueCreateHeaders
import com.microsoft.azure.storage.queue.models.QueueCreateResponse
import com.microsoft.azure.storage.queue.models.QueueDeleteHeaders
import com.microsoft.azure.storage.queue.models.QueueDeleteResponse
import com.microsoft.azure.storage.queue.models.QueueGetAccessPolicyHeaders
import com.microsoft.azure.storage.queue.models.QueueGetPropertiesHeaders
import com.microsoft.azure.storage.queue.models.QueueGetPropertiesResponse
import com.microsoft.azure.storage.queue.models.QueueSetAccessPolicyHeaders
import com.microsoft.azure.storage.queue.models.QueueSetAccessPolicyResponse
import com.microsoft.azure.storage.queue.models.QueueSetMetadataHeaders
import com.microsoft.azure.storage.queue.models.QueueSetMetadataResponse
import com.microsoft.azure.storage.queue.models.ServiceGetStatisticsHeaders
import com.microsoft.azure.storage.queue.models.SignedIdentifier
import com.microsoft.azure.storage.queue.models.StorageErrorCode
import com.microsoft.rest.v2.http.HttpPipeline
import spock.lang.*

import java.time.OffsetDateTime
import java.time.ZoneId


class QueueAPITest extends APISpec {

    ServiceURL su
    QueueURL qu

    def setup() {
        cleanupQueues()
        su = primaryServiceURL
    }

    def "Create Queue All Null"() {
        setup:
        qu = su.createQueueUrl(generateQueueName())

        when:
        QueueCreateResponse response = qu.create(null, null).blockingGet()

        then:
        response.statusCode() == 201
        validateBasicHeaders(response.headers())
    }

    @Unroll
    def "Create metadata"() {
        setup:
        qu = primaryServiceURL.createQueueUrl(generateQueueName())
        Metadata metadata = new Metadata()
        if (key1 != null) {
            metadata.put(key1, value1)
        }
        if (key2 != null) {
            metadata.put(key2, value2)
        }

        when:
        qu.create(metadata, null).blockingGet()
        QueueGetPropertiesResponse response = qu.getProperties(null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.headers().metadata() == metadata

        where:
        key1  | value1 | key2   | value2
        null  | null   | null   | null
        "foo" | "bar"  | "fizz" | "buzz"
    }

    @Unroll
    def "Create Queue Error"() {
        setup:
        qu = primaryServiceURL.createQueueUrl(generateQueueName())

        when:
        QueueCreateResponse response = qu.create(null, null).blockingGet()

        then:
        response.statusCode() == 201
        validateBasicHeaders(response.headers())

        when:
        Metadata metadata = new Metadata()
        metadata.put("Key1", "Value1")
        qu.create(metadata, null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.response().statusCode() == 409
        e.errorCode() == StorageErrorCode.QUEUE_ALREADY_EXISTS
        e.message().contains("The specified queue already exists.")
    }

    def "Create queue min"() {
        expect:
        su.createQueueUrl(generateQueueName()).create().blockingGet().statusCode() == 201
    }

    def "Create queue context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(201, QueueCreateHeaders)))

        def qu = primaryServiceURL.withPipeline(pipeline).createQueueUrl(generateQueueName())

        when:
        // No service call is made. Just satisfy the parameters.
        qu.create(null, defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Get properties null"() {
        setup:
        qu = su.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()

        when:
        QueueGetPropertiesHeaders headers = qu.getProperties(null).blockingGet().headers()

        then:
        validateBasicHeaders(headers)
        headers.metadata().size() == 0
        headers.approximateMessagesCount() == 0
    }

    def "Get properties error"() {
        setup:
        qu = primaryServiceURL.createQueueUrl(generateQueueName())

        when:
        qu.getProperties(null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Get properties min"() {
        setup:
        qu = primaryServiceURL.createQueueUrl(generateQueueName())
        qu.create().blockingGet()

        expect:
        qu.getProperties().blockingGet().statusCode() == 200
    }

    def "Get properties context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(200, QueueGetPropertiesHeaders)))

        def qu = primaryServiceURL.withPipeline(pipeline).createQueueUrl(generateQueueName())

        when:
        // No service call is made. Just satisfy the parameters.
        qu.getProperties(defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Set metadata null"() {
        setup:
        qu = primaryServiceURL.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()
        QueueSetMetadataResponse response = qu.setMetadata(null, null).blockingGet()

        expect:
        response.statusCode() == 204
        validateBasicHeaders(response.headers())
    }

    @Unroll
    def "Set metadata metadata"() {
        setup:
        qu = su.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()
        Metadata metadata = new Metadata()
        if (key1 != null) {
            metadata.put(key1, value1)
        }
        if (key2 != null) {
            metadata.put(key2, value2)
        }

        expect:
        qu.setMetadata(metadata, null).blockingGet().statusCode() == 204
        qu.getProperties(null).blockingGet().headers().metadata() == metadata

        where:
        key1  | value1 | key2   | value2
        null  | null   | null   | null
        "foo" | "bar"  | "fizz" | "buzz"
    }

    def "Set metadata min"() {
        setup:
        qu = primaryServiceURL.createQueueUrl(generateQueueName())
        qu.create().blockingGet()
        Metadata metadata = new Metadata()
        metadata.put("key", "value")

        expect:
        qu.setMetadata(metadata).blockingGet().statusCode() == 204
    }

    def "Set metadata error"() {
        setup:
        qu = primaryServiceURL.createQueueUrl(generateQueueName())

        when:
        qu.setMetadata(null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Set metadata context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(204, QueueSetMetadataHeaders)))

        def qu = primaryServiceURL.withPipeline(pipeline).createQueueUrl(generateQueueName())

        when:
        // No service call is made. Just satisfy the parameters.
        qu.getProperties(defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Set Get Access Policy"() {
        setup:
        qu = su.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()
        SignedIdentifier identifier = new SignedIdentifier()
                .withId("0000")
                .withAccessPolicy(new AccessPolicy()
                .withStart(OffsetDateTime.now().atZoneSameInstant(ZoneId.of("UTC")).toOffsetDateTime())
                .withExpiry(OffsetDateTime.now().atZoneSameInstant(ZoneId.of("UTC")).toOffsetDateTime()
                .plusDays(1))
                .withPermission("r"))
        SignedIdentifier identifier2 = new SignedIdentifier()
                .withId("0001")
                .withAccessPolicy(new AccessPolicy()
                .withStart(OffsetDateTime.now().atZoneSameInstant(ZoneId.of("UTC")).toOffsetDateTime())
                .withExpiry(OffsetDateTime.now().atZoneSameInstant(ZoneId.of("UTC")).toOffsetDateTime()
                .plusDays(2))
                .withPermission("a"))
        List<SignedIdentifier> ids = new ArrayList<>()
        ids.push(identifier)
        ids.push(identifier2)

        when:
        QueueSetAccessPolicyResponse response = qu.setAccessPolicy(ids, null).blockingGet()
        List<SignedIdentifier> receivedIdentifiers = qu.getAccessPolicy().blockingGet().body()

        then:
        response.statusCode() == 204
        validateBasicHeaders(response.headers())
        receivedIdentifiers.get(0).accessPolicy().expiry() == identifier.accessPolicy().expiry()
        receivedIdentifiers.get(0).accessPolicy().start() == identifier.accessPolicy().start()
        receivedIdentifiers.get(0).accessPolicy().permission() == identifier.accessPolicy().permission()
        receivedIdentifiers.get(1).accessPolicy().expiry() == identifier2.accessPolicy().expiry()
        receivedIdentifiers.get(1).accessPolicy().start() == identifier2.accessPolicy().start()
        receivedIdentifiers.get(1).accessPolicy().permission() == identifier2.accessPolicy().permission()
    }

    def "Set get access policy min"() {
        setup:
        qu = su.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()

        expect:
        qu.setAccessPolicy(null).blockingGet().statusCode() == 204
        qu.getAccessPolicy().blockingGet().statusCode() == 200
    }

    def "Set policy error"() {
        setup:
        qu = su.createQueueUrl(generateQueueName())

        when:
        qu.setAccessPolicy(null).blockingGet()

        then:
        thrown(StorageException)
    }


    def "Get policy error"() {
        setup:
        qu = su.createQueueUrl(generateQueueName())

        when:
        qu.getAccessPolicy(null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Set access policy context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(204, QueueSetAccessPolicyHeaders)))

        def qu = primaryServiceURL.withPipeline(pipeline).createQueueUrl(generateQueueName())

        when:
        // No service call is made. Just satisfy the parameters.
        qu.setAccessPolicy(Collections.emptyList(), defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Get access policy context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(200, QueueGetAccessPolicyHeaders)))

        def qu = primaryServiceURL.withPipeline(pipeline).createQueueUrl(generateQueueName())

        when:
        // No service call is made. Just satisfy the parameters.
        qu.getAccessPolicy(defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Delete queue"() {
        setup:
        qu = su.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()

        when:
        QueueDeleteResponse response = qu.delete(null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 204
    }

    def "Delete queue min"() {
        setup:
        qu = su.createQueueUrl(generateQueueName())
        qu.create(null, null).blockingGet()

        expect:
        qu.delete().blockingGet().statusCode() == 204
    }

    def "Delete queue error"() {
        setup:
        qu = su.createQueueUrl(generateQueueName())

        when:
        qu.delete().blockingGet()

        then:
        thrown(StorageException)
    }

    def "Delete queue context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(204, QueueDeleteHeaders)))

        def qu = primaryServiceURL.withPipeline(pipeline).createQueueUrl(generateQueueName())

        when:
        // No service call is made. Just satisfy the parameters.
        qu.delete(defaultContext)

        then:
        notThrown(RuntimeException)
    }
}
