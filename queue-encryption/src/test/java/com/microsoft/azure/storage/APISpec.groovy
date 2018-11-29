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
import com.microsoft.azure.storage.queue.ListQueuesOptions
import com.microsoft.azure.storage.queue.PipelineOptions
import com.microsoft.azure.storage.queue.QueueURL
import com.microsoft.azure.storage.queue.ServiceURL
import com.microsoft.azure.storage.queue.SharedKeyCredentials
import com.microsoft.azure.storage.queue.StorageURL
import com.microsoft.azure.storage.queue.models.QueueItem
import com.microsoft.rest.v2.Context
import com.microsoft.rest.v2.http.HttpClient
import com.microsoft.rest.v2.http.HttpClientConfiguration
import com.microsoft.rest.v2.http.HttpHeaders
import com.microsoft.rest.v2.http.HttpPipeline
import com.microsoft.rest.v2.http.HttpRequest
import com.microsoft.rest.v2.http.HttpResponse
import com.microsoft.rest.v2.policy.RequestPolicy
import com.microsoft.rest.v2.policy.RequestPolicyFactory
import io.reactivex.Flowable
import io.reactivex.Single
import org.spockframework.lang.ISpecificationContext
import spock.lang.Shared
import spock.lang.Specification

import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class APISpec extends Specification {
    @Shared
    Integer iterationNo = 0 // Used to generate stable queue names for recording tests with multiple iterations.

    @Shared
    Integer entityNo = 0 // Used to generate stable queue names for recording tests requiring multiple queues.

    @Shared
    String defaultText = "default"

    @Shared
    ByteBuffer defaultData = ByteBuffer.wrap(defaultText.bytes)

    // If debugging is enabled, recordings cannot run as there can only be one proxy at a time.
    static final boolean enableDebugging = false

    static final String queuePrefix = "jtq" // java test queue

    static final String queueMsgPrefix = "javaQueueMsg"

    static final String keyId = "keyId"

    static SharedKeyCredentials primaryCreds = getGenericCreds("")

    static ServiceURL primaryServiceURL = getGenericServiceURL(primaryCreds)

    static String getTestName(ISpecificationContext ctx) {
        return ctx.getCurrentFeature().name.replace(' ', '').toLowerCase()
    }

    /*
    Constants for testing that the context parameter is properly passed to the pipeline.
     */
    static final String defaultContextKey = "Key"

    static final String defaultContextValue = "Value"

    static final Context defaultContext = new Context(defaultContextKey, defaultContextValue)

    def generateQueueName() {
        generateQueueName(specificationContext, iterationNo, entityNo++)
    }

    def generateMessageText(int size, int entryNumber) {
        StringBuilder sb = new StringBuilder(size)
        Random random = new Random(entryNumber)

        for(long i = 0; i < size; i++) {
            sb.append((char)(random.nextInt(25) + 97))
        }

        return sb.toString()
    }

    def cleanup() {
        cleanupQueues()
    }

    /**
     * This function generates an entity name by concatenating the passed prefix, the name of the test requesting the
     * entity name, and some unique suffix. This ensures that the entity name is unique for each test so there are
     * no conflicts on the service. If we are not recording, we can just use the time. If we are recording, the suffix
     * must always be the same so we can match requests. To solve this, we use the entityNo for how many entities have
     * already been created by this test so far. This would sufficiently distinguish entities within a recording, but
     * could still yield duplicates on the service for data-driven tests. Therefore, we also add the iteration number
     * of the data driven tests.
     *
     * @param specificationContext
     *      Used to obtain the name of the test running.
     * @param prefix
     *      Used to group all entities created by these tests under common prefixes. Useful for listing.
     * @param entityNo
     *      Indicates how man entities have been created by the test so far. This distinguishes multiple queues
     *      created by the same test. Only used when dealing with recordings.
     * @return
     */
    static String generateResourceName(ISpecificationContext specificationContext, String prefix,
                                       int iterationNo, int entityNo) {
        String suffix = ""
        suffix += System.currentTimeMillis() // For uniqueness between runs.
        suffix += entityNo // For easy identification of which call created this resource.
        return prefix + getTestName(specificationContext) + suffix
    }

    static int updateIterationNo(ISpecificationContext specificationContext, int iterationNo) {
        if (specificationContext.currentIteration.estimatedNumIterations > 1) {
            return iterationNo + 1
        } else {
            return 0
        }
    }

    static String generateQueueName(ISpecificationContext specificationContext, int iterationNo, int entityNo) {
        return generateResourceName(specificationContext, queuePrefix, iterationNo, entityNo)
    }

    static String generateMessageText(ISpecificationContext specificationContext, int iterationNo, int entityNo) {
        return generateResourceName(specificationContext, queueMsgPrefix, iterationNo, entityNo)
    }

    static getGenericCreds(String accountType) {
        return new SharedKeyCredentials(System.getenv().get(accountType + "ACCOUNT_NAME"),
                System.getenv().get(accountType + "ACCOUNT_KEY"))
    }

    static HttpClient getHttpClient() {
        if (enableDebugging) {
            HttpClientConfiguration configuration = new HttpClientConfiguration(
                    new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", 8888)))
            return HttpClient.createDefault(configuration)
        } else return HttpClient.createDefault()
    }

    static ServiceURL getGenericServiceURL(SharedKeyCredentials creds) {
        PipelineOptions po = new PipelineOptions().withClient(getHttpClient())

        HttpPipeline pipeline = StorageURL.createPipeline(creds, po)

        return new ServiceURL(new URL("https://" + creds.getAccountName() + ".queue.core.windows.net"), pipeline)
    }

    static void cleanupQueues() throws MalformedURLException {
        HttpPipeline pipeline = StorageURL.createPipeline(primaryCreds, new PipelineOptions())

        ServiceURL serviceURL = new ServiceURL(
                new URL("http://" + System.getenv().get("ACCOUNT_NAME") + ".queue.core.windows.net"), pipeline)
        for (QueueItem q : serviceURL.listQueuesSegment(null,
                new ListQueuesOptions().withPrefix(queuePrefix), null).blockingGet()
                .body().queueItems()) {
            QueueURL queueURL = serviceURL.createQueueUrl(q.name())
            queueURL.delete().blockingGet()
        }
    }

    def setupSpec() {
    }

    /**
     * Validates the presence of headers that are present on a large number of responses. These headers are generally
     * random and can really only be checked as not null.
     * @param headers
     *      The object (may be headers object or response object) that has properties which expose these common headers.
     * @return
     * Whether or not the header values are appropriate.
     */
    def validateBasicHeaders(Object headers) {
        return headers.class.getMethod("requestId").invoke(headers) != null &&
                headers.class.getMethod("version").invoke(headers) != null &&
                headers.class.getMethod("date").invoke(headers) != null
    }

    /*
    This method returns a stub of an HttpResponse. This is for when we want to test policies in isolation but don't care
     about the status code, so we stub a response that always returns a given value for the status code. We never care
     about the number or nature of interactions with this stub.
     */

    def getStubResponse(int code) {
        return Stub(HttpResponse) {
            statusCode() >> code
        }
    }

    /*
    This is for stubbing responses that will actually go through the pipeline and autorest code. Autorest does not seem
    to play too nicely with mocked objects and the complex reflection stuff on both ends made it more difficult to work
    with than was worth it.
     */

    def getStubResponse(int code, Class responseHeadersType) {
        return new HttpResponse() {

            @Override
            int statusCode() {
                return code
            }

            @Override
            String headerValue(String s) {
                return null
            }

            @Override
            HttpHeaders headers() {
                return new HttpHeaders()
            }

            @Override
            Flowable<ByteBuffer> body() {
                return Flowable.empty()
            }

            @Override
            Single<byte[]> bodyAsByteArray() {
                return null
            }

            @Override
            Single<String> bodyAsString() {
                return null
            }

            @Override
            Object deserializedHeaders() {
                return responseHeadersType.getConstructor().newInstance()
            }

            @Override
            boolean isDecoded() {
                return true
            }
        }
    }

    def getSymmetricKey() {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES")
        keyGen.init(256)
        SecretKey secretKey = keyGen.generateKey()
        return new SymmetricKey(keyId, secretKey.getEncoded())
    }
}
