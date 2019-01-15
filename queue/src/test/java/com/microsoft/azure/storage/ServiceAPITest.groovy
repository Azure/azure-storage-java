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

import com.microsoft.azure.storage.queue.ListQueuesOptions
import com.microsoft.azure.storage.queue.PipelineOptions
import com.microsoft.azure.storage.queue.QueueURL
import com.microsoft.azure.storage.queue.QueueURLParts
import com.microsoft.azure.storage.queue.ServiceURL
import com.microsoft.azure.storage.queue.StorageException
import com.microsoft.azure.storage.queue.StorageURL
import com.microsoft.azure.storage.queue.URLParser
import com.microsoft.azure.storage.queue.models.CorsRule
import com.microsoft.azure.storage.queue.models.Logging
import com.microsoft.azure.storage.queue.models.Metrics
import com.microsoft.azure.storage.queue.models.QueueCreateResponse
import com.microsoft.azure.storage.queue.models.QueueItem
import com.microsoft.azure.storage.queue.models.RetentionPolicy
import com.microsoft.azure.storage.queue.models.ServiceGetPropertiesHeaders
import com.microsoft.azure.storage.queue.models.ServiceGetStatisticsHeaders
import com.microsoft.azure.storage.queue.models.ServiceGetStatisticsResponse
import com.microsoft.azure.storage.queue.models.ServiceListQueuesSegmentHeaders
import com.microsoft.azure.storage.queue.models.ServiceListQueuesSegmentResponse
import com.microsoft.azure.storage.queue.models.ServiceSetPropertiesHeaders
import com.microsoft.azure.storage.queue.models.StorageServiceProperties
import com.microsoft.rest.v2.http.HttpPipeline

class ServiceAPITest extends APISpec {
    ServiceURL su
    QueueURL qu

    def setup() {
        APISpec.cleanupQueues()
        su = primaryServiceURL
    }

    def "List Queues Segment"() {
        setup:
        qu = su.createQueueUrl(generateQueueName())

        when:
        QueueCreateResponse cResponse = qu.create(null, null).blockingGet()
        ServiceListQueuesSegmentResponse lResponse =
                su.listQueuesSegment(null, new ListQueuesOptions().withPrefix(queuePrefix), null).blockingGet()

        then:
        cResponse.statusCode() == 201
        validateBasicHeaders(cResponse.headers())
        lResponse.body().queueItems().size() == 1
        for (QueueItem queueItem : lResponse.body().queueItems()) {
            queueItem.name().startsWith(queuePrefix)
        }
    }

    def "List Queue error"() {
        when:
        su.listQueuesSegment("garbage", null, null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "List Queues min"() {
        expect:
        su.listQueuesSegment(null, null).blockingGet().statusCode() == 200
    }

    def "List Queues marker"() {
        setup:
        ArrayList<String> queueNameList = new ArrayList<String>()
        for (int i = 0; i < 10; i++) {
            String queueName = generateQueueName()
            QueueURL qu = primaryServiceURL.createQueueUrl(queueName)
            qu.create(null, null).blockingGet()
            queueNameList.add(queueName)
        }

        ServiceListQueuesSegmentResponse response =
                primaryServiceURL.listQueuesSegment(null,
                        new ListQueuesOptions().withMaxResults(5), null).blockingGet()
        String marker = response.body().nextMarker()
        String firstQueueName = response.body().queueItems().get(0).name()
        response = primaryServiceURL.listQueuesSegment(marker,
                new ListQueuesOptions().withMaxResults(5), null).blockingGet()

        expect:
        // Assert that the second segment is indeed after the first alphabetically
        firstQueueName < response.body().queueItems().get(0).name()

        cleanup:
        for (String queueName : queueNameList) {
            QueueURL queueUrl = primaryServiceURL.createQueueUrl(queueName)
            queueUrl.delete().blockingGet()
        }
    }

    def "List Queues maxResults"() {
        setup:
        ArrayList<String> queueNameList = new ArrayList<String>()
        for (int i = 0; i < 11; i++) {
            String queueName = generateQueueName()
            primaryServiceURL.createQueueUrl(queueName).create(null, null).blockingGet()
            queueNameList.add(queueName)
        }
        expect:
        primaryServiceURL.listQueuesSegment(null,
                new ListQueuesOptions().withMaxResults(10), null)
                .blockingGet().body().queueItems().size() == 10
        cleanup:
        for (String queueName : queueNameList) {
            QueueURL queueUrl = primaryServiceURL.createQueueUrl(queueName)
            queueUrl.delete().blockingGet()
        }
    }

    def validatePropsSet(ServiceSetPropertiesHeaders headers, StorageServiceProperties receivedProperties) {
        return headers.requestId() != null &&
                headers.version() != null &&

                receivedProperties.logging().read() &&
                !receivedProperties.logging().delete() &&
                !receivedProperties.logging().write() &&
                receivedProperties.logging().version() == "1.0" &&
                receivedProperties.logging().retentionPolicy().days() == 5 &&
                receivedProperties.logging().retentionPolicy().enabled() &&

                receivedProperties.cors().size() == 1 &&
                receivedProperties.cors().get(0).allowedMethods() == "GET,PUT,HEAD" &&
                receivedProperties.cors().get(0).allowedHeaders() == "x-ms-version" &&
                receivedProperties.cors().get(0).allowedOrigins() == "*" &&
                receivedProperties.cors().get(0).exposedHeaders() == "x-ms-client-request-id" &&
                receivedProperties.cors().get(0).maxAgeInSeconds() == 10 &&

                receivedProperties.hourMetrics().enabled() &&
                receivedProperties.hourMetrics().includeAPIs() &&
                receivedProperties.hourMetrics().retentionPolicy().enabled() &&
                receivedProperties.hourMetrics().retentionPolicy().days() == 5 &&
                receivedProperties.hourMetrics().version() == "1.0" &&

                receivedProperties.minuteMetrics().enabled() &&
                receivedProperties.minuteMetrics().includeAPIs() &&
                receivedProperties.minuteMetrics().retentionPolicy().enabled() &&
                receivedProperties.minuteMetrics().retentionPolicy().days() == 5 &&
                receivedProperties.minuteMetrics().version() == "1.0"
    }

    def "List queues context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(200, ServiceListQueuesSegmentHeaders)))

        def su = primaryServiceURL.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        su.listQueuesSegment(null, null, defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Set props min"() {
        setup:
        RetentionPolicy retentionPolicy = new RetentionPolicy().withDays(5).withEnabled(true)
        Logging logging = new Logging().withRead(true).withVersion("1.0")
                .withRetentionPolicy(retentionPolicy)
        expect:
        su.setProperties(new StorageServiceProperties().withLogging(logging)).blockingGet().statusCode() == 202
    }

    def "Get props min"() {
        expect:
        su.getProperties().blockingGet().statusCode() == 200
    }

    def "Set get properties"() {
        when:
        RetentionPolicy retentionPolicy = new RetentionPolicy().withDays(5).withEnabled(true)
        Logging logging = new Logging().withRead(true).withVersion("1.0")
                .withRetentionPolicy(retentionPolicy)
        ArrayList<CorsRule> corsRules = new ArrayList<>()
        corsRules.add(new CorsRule().withAllowedMethods("GET,PUT,HEAD")
                .withAllowedOrigins("*")
                .withAllowedHeaders("x-ms-version")
                .withExposedHeaders("x-ms-client-request-id")
                .withMaxAgeInSeconds(10))
        Metrics hourMetrics = new Metrics().withEnabled(true).withVersion("1.0")
                .withRetentionPolicy(retentionPolicy).withIncludeAPIs(true)
        Metrics minuteMetrics = new Metrics().withEnabled(true).withVersion("1.0")
                .withRetentionPolicy(retentionPolicy).withIncludeAPIs(true)


        ServiceSetPropertiesHeaders headers = primaryServiceURL.setProperties(new StorageServiceProperties()
                .withLogging(logging).withCors(corsRules).withMinuteMetrics(minuteMetrics).withHourMetrics(hourMetrics), null).blockingGet().headers()
        StorageServiceProperties receivedProperties = primaryServiceURL.getProperties(null).blockingGet().body()

        then:
        if (!validatePropsSet(headers, receivedProperties)) {
            // Service properties may take up to 30s to take effect. If they weren't already in place, wait.
            sleep(30 * 1000)
            validatePropsSet(headers, receivedProperties)
        }
    }

    def "Set properties context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(202, ServiceSetPropertiesHeaders)))

        def su = primaryServiceURL.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        su.setProperties(new StorageServiceProperties(), defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Get props context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(200, ServiceGetPropertiesHeaders)))

        def su = primaryServiceURL.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        su.getProperties(defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Get stats"() {
        setup:
        QueueURLParts parts = URLParser.parse(primaryServiceURL.toURL())
        parts.withHost(primaryCreds.accountName +  "-secondary.queue.core.windows.net")

        when:
        ServiceURL secondary = new ServiceURL(parts.toURL(),
                StorageURL.createPipeline(primaryCreds, new PipelineOptions()))
        ServiceGetStatisticsResponse response = secondary.getStatistics(null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 200
        response.body().geoReplication().status() != null
        response.body().geoReplication().lastSyncTime() != null
    }

    def "Get stats min"() {
        setup:
        QueueURLParts parts = URLParser.parse(primaryServiceURL.toURL())
        parts.withHost(primaryCreds.accountName +  "-secondary.queue.core.windows.net")
        ServiceURL secondary = new ServiceURL(parts.toURL(),
                StorageURL.createPipeline(primaryCreds, new PipelineOptions()))
        expect:
        secondary.getStatistics().blockingGet().statusCode() == 200
    }

    def "Get stats error"() {
        when:
        primaryServiceURL.getStatistics(null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Get stats context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(200, ServiceGetStatisticsHeaders)))

        def su = primaryServiceURL.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        su.getStatistics(defaultContext)

        then:
        notThrown(RuntimeException)
    }
}
