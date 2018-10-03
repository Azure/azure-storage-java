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
import java.time.temporal.ChronoUnit;
import java.util.List;

import static com.microsoft.azure.storage.queue.Utility.addErrorWrappingToSingle;

/**
 * Represents a URL to a queue. It may be obtained by direct construction or via the create method on a
 * {@link ServiceURL} object. This class does not hold any state about messages in the queue but is instead a convenient way
 * of sending off appropriate requests to the resource on the service. It may also be used to construct URLs to messages in the queue.
 * Please refer to the
 * <a href=https://docs.microsoft.com/en-us/rest/api/storageservices/queue-service-rest-api>Azure Docs</a>
 * for more information on queues.
 */

public final class QueueURL extends StorageURL {

    public QueueURL(URL url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link QueueURL} with the given pipeline.
     *
     * @param pipeline
     *         An {@link HttpPipeline} object to set.
     *
     * @return A {@link QueueURL} object with the given pipeline.
     */
    public QueueURL withPipeline(HttpPipeline pipeline) {
        try {
            return new QueueURL(new URL(this.storageClient.url()), pipeline);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * createMessagesUrl creates a new MessagesURL object by concatenating "messages" to the end of
     * QueueURL's URL. The new MessagesURL uses the same request policy pipeline as the QueueURL.
     * To change the pipeline, create the MessagesURL and then call its WithPipeline method passing in the
     * desired pipeline object.
     *
     * @return A {@link MessagesURL} object with existing queue's pipeline.
     */
    public MessagesURL createMessagesUrl() {
        try {
            return new MessagesURL(StorageURL.appendToURLPath(new URL(this.storageClient.url()), "messages"),
                    this.storageClient.httpPipeline());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a queue within a storage account.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/create-queue4">Azure Docs</a>.
     *
     * @return Emits the successful response.
     */
    public Single<QueueCreateResponse> create() {
        return this.create(null, null);
    }

    /**
     * Create a queue within a storage account.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/create-queue4">Azure Docs</a>.
     *
     * @param metadata
     *         {@link Metadata}
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<QueueCreateResponse> create(Metadata metadata, Context context) {
        metadata = metadata == null ? Metadata.NONE : metadata;
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedQueues().createWithRestResponseAsync(
                context, null, metadata, null));
    }

    /**
     * Delete permanently deletes a queue.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/delete-queue3">Azure Docs</a>.
     *
     * @return Emits the successful response.
     */
    public Single<QueueDeleteResponse> delete() {
        return this.delete(null);
    }

    /**
     * Delete permanently deletes a queue.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/delete-queue3">Azure Docs</a>.
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
    public Single<QueueDeleteResponse> delete(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedQueues()
                .deleteWithRestResponseAsync(context, null, null));
    }

    /**
     * GetProperties retrieves queue properties and user-defined metadata and properties on the specified queue.
     * Metadata is associated with the queue as name-values pairs.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-queue-metadata">Azure Docs</a>.
     *
     * @return Emits the successful response.
     */
    public Single<QueueGetPropertiesResponse> getProperties() {
        return this.getProperties(null);
    }

    /**
     * GetProperties retrieves queue properties and user-defined metadata and properties on the specified queue.
     * Metadata is associated with the queue as name-values pairs.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-queue-metadata">Azure Docs</a>.
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
    public Single<QueueGetPropertiesResponse> getProperties(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedQueues().
                getPropertiesWithRestResponseAsync(context, null, null));
    }

    /**
     * SetMetadata sets user-defined metadata on the specified queue. Metadata is associated with the queue as
     * name-value pairs. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/set-queue-metadata">Azure Docs</a>.
     *
     * @param metadata
     *         {@link Metadata}
     *
     * @return Emits the successful response.
     */
    public Single<QueueSetMetadataResponse> setMetadata(Metadata metadata) {
        return this.setMetadata(metadata, null);
    }

    /**
     * SetMetadata sets user-defined metadata on the specified queue. Metadata is associated with the queue as
     * name-value pairs. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/set-queue-metadata">Azure Docs</a>.
     *
     * @param metadata
     *         {@link Metadata}
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<QueueSetMetadataResponse> setMetadata(Metadata metadata, Context context) {
        metadata = metadata == null ? Metadata.NONE : metadata;
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedQueues().setMetadataWithRestResponseAsync(context, null, metadata, null));
    }

    /**
     * GetAccessPolicy returns details about any stored access policies specified on the queue that may be used with
     * Shared Access Signatures. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-queue-acl">Azure Docs</a>.
     *
     * @return Emits the successful response.
     */
    public Single<QueueGetAccessPolicyResponse> getAccessPolicy() {
        return this.getAccessPolicy(null);
    }

    /**
     * GetAccessPolicy returns details about any stored access policies specified on the queue that may be used with
     * Shared Access Signatures. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-queue-acl">Azure Docs</a>.
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
    public Single<QueueGetAccessPolicyResponse> getAccessPolicy(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedQueues().
                getAccessPolicyWithRestResponseAsync(context, null, null));
    }

    /**
     * The Set Queue ACL operation sets stored access policies for the queue that may be used with Shared Access Signatures.
     * Note that, for each signed identifier, we will truncate the start and expiry times to the nearest second to
     * ensure the time formatting is compatible with the service. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/set-queue-acl">Azure Docs</a>.
     *
     * @param identifiers
     *         A list of {@link SignedIdentifier} objects that specify the permissions for the queue. Please see
     *         <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/establishing-a-stored-access-policy">here</a>
     *         for more information. Passing null will clear all access policies.
     *
     * @return Emits the successful response.
     */
    public Single<QueueSetAccessPolicyResponse> setAccessPolicy(List<SignedIdentifier> identifiers) {
        return this.setAccessPolicy(identifiers, null);
    }

    /**
     * The Set Queue ACL operation sets stored access policies for the queue that may be used with Shared Access Signatures.
     * Note that, for each signed identifier, we will truncate the start and expiry times to the nearest second to
     * ensure the time formatting is compatible with the service. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/set-queue-acl">Azure Docs</a>.
     *
     * @param identifiers
     *         A list of {@link SignedIdentifier} objects that specify the permissions for the queue. Please see
     *         <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/establishing-a-stored-access-policy">here</a>
     *         for more information. Passing null will clear all access policies.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<QueueSetAccessPolicyResponse> setAccessPolicy(List<SignedIdentifier> identifiers, Context context) {
        context = context == null ? Context.NONE : context;

        if (identifiers != null) {
            for (SignedIdentifier identifier : identifiers) {
                if (identifier.accessPolicy() != null && identifier.accessPolicy().start() != null) {
                    identifier.accessPolicy().withStart(
                            identifier.accessPolicy().start().truncatedTo(ChronoUnit.SECONDS));
                }
                if (identifier.accessPolicy() != null && identifier.accessPolicy().expiry() != null) {
                    identifier.accessPolicy().withExpiry(
                            identifier.accessPolicy().expiry().truncatedTo(ChronoUnit.SECONDS));
                }
            }
        }
        return addErrorWrappingToSingle(this.storageClient.generatedQueues().
                setAccessPolicyWithRestResponseAsync(context, identifiers, null, null));
    }
}
