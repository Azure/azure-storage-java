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
import java.util.Collections;
import java.util.List;

import static com.microsoft.azure.storage.queue.Utility.*;

/**
 * Represents a URL to a queue service. This class does not hold any state about a particular storage account but is
 * instead a convenient way of sending off appropriate requests to the resource on the service.
 * It may also be used to construct URLs to queues .
 * Please see <a href=https://docs.microsoft.com/en-us/azure/storage/queues/storage-queues-introduction>here</a> for more
 * information on queues.
 */
public final class ServiceURL extends StorageURL {

    /**
     * Creates a {@code ServiceURL} object pointing to the account specified by the URL and using the provided pipeline
     * to make HTTP requests.
     *
     * @param url
     *         A url to an Azure Storage account.
     * @param pipeline
     *         A pipeline which configures the behavior of HTTP exchanges. Please refer to the createPipeline method on
     *         {@link StorageURL} for more information.
     */
    public ServiceURL(URL url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a {@link QueueURL} object pointing a queue with given queue name uner the current storage account.
     *
     * @param queueName
     *         The name of the queue which the URL will point to.
     *
     * @return A {@link QueueURL} object.
     */
    public QueueURL createQueueUrl(String queueName) {
        try {
            return new QueueURL(StorageURL.appendToURLPath(new URL(super.storageClient.url()), queueName),
                    super.storageClient.httpPipeline());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new {@link ServiceURL} with the given pipeline.
     *
     * @param pipeline
     *         An {@link HttpPipeline} object to set.
     *
     * @return A {@link ServiceURL} object with the given pipeline.
     */
    public ServiceURL withPipeline(HttpPipeline pipeline) {
        try {
            return new ServiceURL(new URL(super.storageClient.url()), pipeline);
        } catch (MalformedURLException e) {
            // TODO: remove
        }
        return null;
    }

    /**
     * Returns a single segment of queues starting from the specified Marker.
     * Use an empty marker to start enumeration from the beginning. Queue names are returned in lexicographic order.
     * After getting a segment, process it, and then call ListQueue again (passing the the previously-returned
     * Marker) to get the next segment. For more information, see
     * the <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/list-queues1">Azure Docs</a>.
     *
     * @param marker
     *         Identifies the portion of the list to be returned with the next list operation.
     *         This value is returned in the response of a previous list operation as the
     *         ListContainersSegmentResponse.body().nextMarker(). Set to null to list the first segment.
     * @param options
     *         A {@link ListQueuesOptions} which specifies what data should be returned by the service.
     *
     * @return Emits the successful response.
     */
    public Single<ServiceListQueuesSegmentResponse> listQueuesSegment(String marker, ListQueuesOptions options) {
        return this.listQueuesSegment(marker, options, null);
    }

    /**
     * Returns a single segment of queues starting from the specified Marker.
     * Use an empty marker to start enumeration from the beginning. Queue names are returned in lexicographic order.
     * After getting a segment, process it, and then call ListQueue again (passing the the previously-returned
     * Marker) to get the next segment. For more information, see
     * the <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/list-queues1">Azure Docs</a>.
     *
     * @param marker
     *         Identifies the portion of the list to be returned with the next list operation.
     *         This value is returned in the response of a previous list operation as the
     *         ListContainersSegmentResponse.body().nextMarker(). Set to null to list the first segment.
     * @param options
     *         A {@link ListQueuesOptions} which specifies what data should be returned by the service.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ServiceListQueuesSegmentResponse> listQueuesSegment(
            String marker, ListQueuesOptions options, Context context) {
        context = context == null ? Context.NONE : context;
        options = options == null ? ListQueuesOptions.DEFAULT : options;
        List<ListQueuesIncludeType> includeTypeList = options.details() == null ? Collections.emptyList() : options.details().toIncludeTypeList();
        return addErrorWrappingToSingle(
                this.storageClient.generatedServices().
                        listQueuesSegmentWithRestResponseAsync(context, options.prefix(), marker, options.maxResults(),
                                includeTypeList, null, null));
    }

    /**
     * Gets the properties of a storage account’s queue service. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-queue-service-properties">Azure Docs</a>.
     *
     * @return Emits the successful response.
     */
    public Single<ServiceGetPropertiesResponse> getProperties() {
        return this.getProperties(null);
    }

    /**
     * Gets the properties of a storage account’s queue service. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-queue-service-properties">Azure Docs</a>.
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
    public Single<ServiceGetPropertiesResponse> getProperties(Context context) {
        return addErrorWrappingToSingle(
                this.storageClient.generatedServices().getPropertiesWithRestResponseAsync(context, null, null));
    }

    /**
     * Sets properties for a storage account's queue service endpoint. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/set-queue-service-properties">Azure Docs</a>.
     * Note that setting the default service version has no effect when using this client because this client explicitly
     * sets the version header on each request, overriding the default.
     *
     * @param properties
     *         Configures the service.
     *
     * @return Emits the successful response.
     */
    public Single<ServiceSetPropertiesResponse> setProperties(StorageServiceProperties properties) {
        return this.setProperties(properties, null);
    }

    /**
     * Sets properties for a storage account's queue service endpoint. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/set-queue-service-properties">Azure Docs</a>.
     * Note that setting the default service version has no effect when using this client because this client explicitly
     * sets the version header on each request, overriding the default.
     *
     * @param properties
     *         Configures the service.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ServiceSetPropertiesResponse> setProperties(StorageServiceProperties properties, Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(
                this.storageClient.generatedServices().
                        setPropertiesWithRestResponseAsync(context, properties, null, null));
    }

    /**
     * Retrieves statistics related to replication for the queue service. It is
     * only available on the secondary location endpoint when read-access geo-redundant
     * replication is enabled for the storage account.. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-queue-service-stats">Azure Docs</a>.
     *
     * @return Emits the successful response.
     */
    public Single<ServiceGetStatisticsResponse> getStatistics() {
        return this.getStatistics(null);
    }

    /**
     * Retrieves statistics related to replication for the queue service. It is
     * only available on the secondary location endpoint when read-access geo-redundant
     * replication is enabled for the storage account.. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-queue-service-stats">Azure Docs</a>.
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
    public Single<ServiceGetStatisticsResponse> getStatistics(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(
                this.storageClient.generatedServices().getStatisticsWithRestResponseAsync(context, null, null));
    }
}
