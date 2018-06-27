/**
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
package com.microsoft.azure.storage.blob;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.Cipher;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.DoesServiceRequest;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.core.Base64;
import com.microsoft.azure.storage.core.BaseResponse;
import com.microsoft.azure.storage.core.ExecutionEngine;
import com.microsoft.azure.storage.core.RequestLocationMode;
import com.microsoft.azure.storage.core.SR;
import com.microsoft.azure.storage.core.StorageRequest;
import com.microsoft.azure.storage.core.UriQueryBuilder;
import com.microsoft.azure.storage.core.Utility;

/**
 * Represents a Microsoft Azure page blob.
 */
public final class CloudPageBlob extends CloudBlob {
    /**
     * Creates an instance of the <code>CloudPageBlob</code> class using the specified absolute URI and storage service
     * client.
     * 
     * @param blobAbsoluteUri
     *            A <code>java.net.URI</code> object which represents the absolute URI to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public CloudPageBlob(final URI blobAbsoluteUri) throws StorageException {
        this(new StorageUri(blobAbsoluteUri));
    }

    /**
     * Creates an instance of the <code>CloudPageBlob</code> class using the specified absolute URI and storage service
     * client.
     * 
     * @param blobAbsoluteUri
     *            A {@link StorageUri} object which represents the absolute URI to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public CloudPageBlob(final StorageUri blobAbsoluteUri) throws StorageException {
        this(blobAbsoluteUri, (StorageCredentials)null);
    }

    /**
     * Creates an instance of the <code>CloudPageBlob</code> class by copying values from another page blob.
     * 
     * @param otherBlob
     *            A <code>CloudPageBlob</code> object which represents the page blob to copy.
     */
    public CloudPageBlob(final CloudPageBlob otherBlob) {
        super(otherBlob);
    }
    
    /**
     * Creates an instance of the <code>CloudPageBlob</code> class using the specified absolute URI and credentials.
     * 
     * @param blobAbsoluteUri
     *            A <code>java.net.URI</code> object that represents the absolute URI to the blob.
     * @param credentials
     *            A {@link StorageCredentials} object used to authenticate access.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public CloudPageBlob(final URI blobAbsoluteUri, final StorageCredentials credentials) throws StorageException {
        this(new StorageUri(blobAbsoluteUri), credentials);
    }

    /**
     * Creates an instance of the <code>CloudPageBlob</code> class using the specified absolute URI, snapshot ID, and
     * credentials.
     * 
     * @param blobAbsoluteUri
     *            A <code>java.net.URI</code> object that represents the absolute URI to the blob.
     * @param snapshotID
     *            A <code>String</code> that represents the snapshot version, if applicable.
     * @param credentials
     *            A {@link StorageCredentials} object used to authenticate access.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public CloudPageBlob(final URI blobAbsoluteUri, final String snapshotID, final StorageCredentials credentials)
            throws StorageException {
        this(new StorageUri(blobAbsoluteUri), snapshotID, credentials);
    }
    
    /**
     * Creates an instance of the <code>CloudPageBlob</code> class using the specified absolute StorageUri and credentials.
     * 
     * @param blobAbsoluteUri
     *            A {@link StorageUri} object that represents the absolute URI to the blob.
     * @param credentials
     *            A {@link StorageCredentials} object used to authenticate access.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public CloudPageBlob(final StorageUri blobAbsoluteUri, final StorageCredentials credentials) throws StorageException {
        this(blobAbsoluteUri, null /* snapshotID */, credentials);
    }

    /**
     * Creates an instance of the <code>CloudPageBlob</code> class using the specified absolute StorageUri, snapshot
     * ID, and credentials.
     * 
     * @param blobAbsoluteUri
     *            A {@link StorageUri} object that represents the absolute URI to the blob.
     * @param snapshotID
     *            A <code>String</code> that represents the snapshot version, if applicable.
     * @param credentials
     *            A {@link StorageCredentials} object used to authenticate access.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public CloudPageBlob(final StorageUri blobAbsoluteUri, final String snapshotID, final StorageCredentials credentials)
            throws StorageException {
        super(BlobType.PAGE_BLOB, blobAbsoluteUri, snapshotID, credentials);
    }
    
    /**
     * Creates an instance of the <code>CloudPageBlob</code> class using the specified type, name, snapshot ID, and
     * container.
     *
     * @param blobName
     *            Name of the blob.
     * @param snapshotID
     *            A <code>String</code> that represents the snapshot version, if applicable.
     * @param container
     *            The reference to the parent container.
     * @throws URISyntaxException
     *             If the resource URI is invalid.
     */
    protected CloudPageBlob(String blobName, String snapshotID, CloudBlobContainer container)
            throws URISyntaxException {
        super(BlobType.PAGE_BLOB, blobName, snapshotID, container);
    }
    
    /**
     * Requests the service to start copying a blob's contents, properties, and metadata to a new blob.
     *
     * @param sourceBlob
     *            A <code>CloudPageBlob</code> object that represents the source blob to copy.
     *
     * @return A <code>String</code> which represents the copy ID associated with the copy operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws URISyntaxException
     */
    @DoesServiceRequest
    public final String startCopy(final CloudPageBlob sourceBlob) throws StorageException, URISyntaxException {
        return this.startCopy(sourceBlob, null /* sourceAccessCondition */,
                null /* destinationAccessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Requests the service to start copying a blob's contents, properties, and metadata to a new blob, using the
     * specified access conditions, lease ID, request options, and operation context.
     *
     * @param sourceBlob
     *            A <code>CloudPageBlob</code> object that represents the source blob to copy.
     * @param sourceAccessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the source blob.
     * @param destinationAccessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the destination blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @return A <code>String</code> which represents the copy ID associated with the copy operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws URISyntaxException
     *
     */
    @DoesServiceRequest
    public final String startCopy(final CloudPageBlob sourceBlob, final AccessCondition sourceAccessCondition,
                                  final AccessCondition destinationAccessCondition, BlobRequestOptions options, OperationContext opContext)
            throws StorageException, URISyntaxException {
        return this.startCopy(sourceBlob, null /* premiumBlobTier */, sourceAccessCondition, destinationAccessCondition, options, opContext);
    }

    /**
     * Requests the service to start copying a blob's contents, properties, and metadata to a new blob, using the
     * specified blob tier, access conditions, lease ID, request options, and operation context.
     *
     * @param sourceBlob
     *            A <code>CloudPageBlob</code> object that represents the source blob to copy.
     * @param premiumBlobTier
     *            A {@link PremiumPageBlobTier} object which represents the tier of the blob.
     * @param sourceAccessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the source blob.
     * @param destinationAccessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the destination blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @return A <code>String</code> which represents the copy ID associated with the copy operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws URISyntaxException
     *
     */
    @DoesServiceRequest
    public final String startCopy(final CloudPageBlob sourceBlob, final PremiumPageBlobTier premiumBlobTier, final AccessCondition sourceAccessCondition,
            final AccessCondition destinationAccessCondition, BlobRequestOptions options, OperationContext opContext)
            throws StorageException, URISyntaxException {
        Utility.assertNotNull("sourceBlob", sourceBlob);

        URI source = sourceBlob.getSnapshotQualifiedUri();
        if (sourceBlob.getServiceClient() != null && sourceBlob.getServiceClient().getCredentials() != null)
        {
            source = sourceBlob.getServiceClient().getCredentials().transformUri(sourceBlob.getSnapshotQualifiedUri());
        }

        return this.startCopy(source, premiumBlobTier, sourceAccessCondition, destinationAccessCondition, options, opContext);
    }

    /**
     * Requests the service to start an incremental copy of another page blob's contents, properties, and metadata
     * to this blob.
     *
     * @param sourceSnapshot
     *            A <code>CloudPageBlob</code> object that represents the source blob to copy. Must be a snapshot.
     *
     * @return A <code>String</code> which represents the copy ID associated with the copy operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws URISyntaxException
     */
    @DoesServiceRequest
    public final String startIncrementalCopy(final CloudPageBlob sourceSnapshot) throws StorageException, URISyntaxException {
        final UriQueryBuilder builder = new UriQueryBuilder();
        builder.add(Constants.QueryConstants.SNAPSHOT, sourceSnapshot.snapshotID);
        URI sourceUri = builder.addToURI(sourceSnapshot.getTransformedAddress(null).getPrimaryUri());

        return this.startIncrementalCopy(sourceUri, null /* destinationAccessCondition */,
                null /* options */, null /* opContext */);
    }

    /**
     * Requests the service to start an incremental copy of another page blob's contents, properties, and metadata
     * to this blob.
     *
     * @param sourceSnapshot
     *            A <code>CloudPageBlob</code> object that represents the source blob to copy. Must be a snapshot.
     *
     * @return A <code>String</code> which represents the copy ID associated with the copy operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws URISyntaxException
     */
    @DoesServiceRequest
    public final String startIncrementalCopy(final URI sourceSnapshot) throws StorageException, URISyntaxException {
        return this.startIncrementalCopy(sourceSnapshot, null /* destinationAccessCondition */,
                null /* options */, null /* opContext */);
    }

    /**
     * Requests the service to start copying a blob's contents, properties, and metadata to a new blob, using the
     * specified access conditions, lease ID, request options, and operation context.
     *
     * @param sourceSnapshot
     *            A <code>CloudPageBlob</code> object that represents the source blob to copy. Must be a snapshot.
     * @param destinationAccessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the destination blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @return A <code>String</code> which represents the copy ID associated with the copy operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws URISyntaxException
     *
     */
    @DoesServiceRequest
    public final String startIncrementalCopy(final CloudPageBlob sourceSnapshot, 
            final AccessCondition destinationAccessCondition, BlobRequestOptions options, OperationContext opContext)
            throws StorageException, URISyntaxException {
        final UriQueryBuilder builder = new UriQueryBuilder();
        builder.add(Constants.QueryConstants.SNAPSHOT, sourceSnapshot.snapshotID);

        URI sourceUri = builder.addToURI(sourceSnapshot.getTransformedAddress(null).getPrimaryUri());
        return this.startIncrementalCopy(sourceUri, destinationAccessCondition, options, opContext);
    }

    /**
     * Requests the service to start copying a blob's contents, properties, and metadata to a new blob, using the
     * specified access conditions, lease ID, request options, and operation context.
     *
     * @param sourceSnapshot
     *            A <code>CloudPageBlob</code> object that represents the source blob to copy. Must be a snapshot.
     * @param destinationAccessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the destination blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @return A <code>String</code> which represents the copy ID associated with the copy operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws URISyntaxException
     *
     */
    @DoesServiceRequest
    public final String startIncrementalCopy(final URI sourceSnapshot,
            final AccessCondition destinationAccessCondition, BlobRequestOptions options, OperationContext opContext)
            throws StorageException, URISyntaxException {
        Utility.assertNotNull("sourceSnapshot", sourceSnapshot);
        this.assertNoWriteOperationForSnapshot();
        
        if (opContext == null) {
            opContext = new OperationContext();
        }

        opContext.initialize();
        options = BlobRequestOptions.populateAndApplyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

        return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                this.startCopyImpl(sourceSnapshot, Constants.EMPTY_STRING,false /* syncCopy */, true /* incrementalCopy */, null /* premiumPageBlobTier */, null /* sourceAccesCondition */,
                        destinationAccessCondition, options),
                options.getRetryPolicyFactory(), opContext);
    }

    /**
     * Clears pages from a page blob.
     * <p>
     * Calling <code>clearPages</code> releases the storage space used by the specified pages. Pages that have been
     * cleared are no longer tracked as part of the page blob, and no longer incur a charge against the storage account.
     * 
     * @param offset
     *            The offset, in bytes, at which to begin clearing pages. This value must be a multiple of 512.
     * @param length
     *            The length, in bytes, of the data range to be cleared. This value must be a multiple of 512.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void clearPages(final long offset, final long length) throws StorageException {
        this.clearPages(offset, length, null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Clears pages from a page blob using the specified lease ID, request options, and operation context.
     * <p>
     * Calling <code>clearPages</code> releases the storage space used by the specified pages. Pages that have been
     * cleared are no longer tracked as part of the page blob, and no longer incur a charge against the storage account.
     * 
     * @param offset
     *            A <code>long</code> which represents the offset, in bytes, at which to begin clearing pages. This
     *            value must be a multiple of 512.
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the data range to be cleared. This value
     *            must be a multiple of 512.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void clearPages(final long offset, final long length, final AccessCondition accessCondition,
            BlobRequestOptions options, OperationContext opContext) throws StorageException {
        if (offset % Constants.PAGE_SIZE != 0) {
            throw new IllegalArgumentException(SR.INVALID_PAGE_START_OFFSET);
        }

        if (length % Constants.PAGE_SIZE != 0) {
            throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
        }

        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);

        // Assert no encryption policy as this is not supported for partial uploads
        options.assertNoEncryptionPolicyOrStrictMode();

        PageRange range = new PageRange(offset, offset + length - 1);

        this.putPagesInternal(range, PageOperationType.CLEAR, null, length, null, accessCondition, options, opContext);
    }

    /**
     * Creates a page blob. If the blob already exists, this will replace it. To instead throw an error if the blob 
     * already exists, use the {@link #create(long, AccessCondition, BlobRequestOptions, OperationContext)}
     * overload with {@link AccessCondition#generateIfNotExistsCondition()}.
     * @param length
     *            A <code>long</code> which represents the size, in bytes, of the page blob.
     * 
     * @throws IllegalArgumentException
     *             If the length is not a multiple of 512.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void create(final long length) throws StorageException {
        this.create(length, null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Creates a page blob using the specified request options and operation context. If the blob already exists,
     * this will replace it. To instead throw an error if the blob already exists, use
     * {@link AccessCondition#generateIfNotExistsCondition()}.
     *
     * @param length
     *            A <code>long</code> which represents the size, in bytes, of the page blob.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @throws IllegalArgumentException
     *             If the length is not a multiple of 512.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void create(final long length, final AccessCondition accessCondition, BlobRequestOptions options,
                       OperationContext opContext) throws StorageException {
        this.create(length, null /* premiumBlobTier */, accessCondition, options, opContext);
    }

    /**
     * Creates a page blob using the specified request options and operation context. If the blob already exists, 
     * this will replace it. To instead throw an error if the blob already exists, use 
     * {@link AccessCondition#generateIfNotExistsCondition()}.
     * 
     * @param length
     *            A <code>long</code> which represents the size, in bytes, of the page blob.
     * @param premiumBlobTier
     *            A {@link PremiumPageBlobTier} object which represents the tier of the blob.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws IllegalArgumentException
     *             If the length is not a multiple of 512.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void create(final long length, final PremiumPageBlobTier premiumBlobTier, final AccessCondition accessCondition, BlobRequestOptions options,
            OperationContext opContext) throws StorageException {
        assertNoWriteOperationForSnapshot();

        if (length % Constants.PAGE_SIZE != 0) {
            throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
        }

        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);

        ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                this.createImpl(length, premiumBlobTier, accessCondition, options), options.getRetryPolicyFactory(), opContext);
    }

    private StorageRequest<CloudBlobClient, CloudBlob, Void> createImpl(final long length, final PremiumPageBlobTier premiumBlobTier,
            final AccessCondition accessCondition, final BlobRequestOptions options) {
        final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                options, this.getStorageUri()) {

            @Override
            public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                    throws Exception {
                return BlobRequest.putBlob(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                        options, context, accessCondition, blob.properties, BlobType.PAGE_BLOB, length, premiumBlobTier);
            }

            @Override
            public void setHeaders(HttpURLConnection connection, CloudBlob blob, OperationContext context) {
                BlobRequest.addMetadata(connection, blob.metadata, context);
            }

            @Override
            public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                    throws Exception {
                StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, context);
            }

            @Override
            public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                    throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                    this.setNonExceptionedRetryableFailure(true);
                    return null;
                }

                blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                this.getResult().setRequestServiceEncrypted(BaseResponse.isServerRequestEncrypted(this.getConnection()));
                blob.getProperties().setLength(length);
                blob.getProperties().setPremiumPageBlobTier(premiumBlobTier);
                if (premiumBlobTier != null) {
                    blob.getProperties().setBlobTierInferred(false);
                }

                return null;
            }

        };

        return putRequest;
    }

    /**
     * Returns a collection of page ranges and their starting and ending byte offsets.
     * <p>
     * The start and end byte offsets for each page range are inclusive.
     * 
     * @return An <code>ArrayList</code> object which represents the set of page ranges and their starting and ending
     *         byte offsets.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public ArrayList<PageRange> downloadPageRanges() throws StorageException {
        return this.downloadPageRanges(null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Returns a collection of page ranges and their starting and ending byte offsets using the specified request
     * options and operation context.
     * 
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return An <code>ArrayList</code> object which represents the set of page ranges and their starting and ending
     *         byte offsets.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public ArrayList<PageRange> downloadPageRanges(final AccessCondition accessCondition, BlobRequestOptions options,
            OperationContext opContext) throws StorageException {

        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);

        return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                this.downloadPageRangesImpl(null /* offset */, null /* length */, accessCondition, options),
                options.getRetryPolicyFactory(), opContext);
    }

    /**
     * Returns a collection of page ranges and their starting and ending byte offsets.
     *
     * @param offset
     *            The starting offset of the data range over which to list page ranges, in bytes. Must be a multiple of
     *            512.
     * @param length
     *            The length of the data range over which to list page ranges, in bytes. Must be a multiple of 512.
     * @return A <code>List</code> object which represents the set of page ranges and their starting and ending
     *         byte offsets.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public List<PageRange> downloadPageRanges(final long offset, final Long length) throws StorageException {
        return this.downloadPageRanges(offset, length, null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Returns a collection of page ranges and their starting and ending byte offsets using the specified request
     * options and operation context.
     * 
     * @param offset
     *            The starting offset of the data range over which to list page ranges, in bytes. Must be a multiple of
     *            512.
     * @param length
     *            The length of the data range over which to list page ranges, in bytes. Must be a multiple of 512.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return A <code>List</code> object which represents the set of page ranges and their starting and ending
     *         byte offsets.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public List<PageRange> downloadPageRanges(final long offset, final Long length,
            final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
            throws StorageException {
        if (offset < 0 || (length != null && length <= 0)) {
            throw new IndexOutOfBoundsException();
        }
        
        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);

        return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                this.downloadPageRangesImpl(offset, length, accessCondition, options), options.getRetryPolicyFactory(), opContext);
    }
    
    private StorageRequest<CloudBlobClient, CloudBlob, ArrayList<PageRange>> downloadPageRangesImpl(final Long offset,
            final Long length, final AccessCondition accessCondition, final BlobRequestOptions options) {
        final StorageRequest<CloudBlobClient, CloudBlob, ArrayList<PageRange>> getRequest = new StorageRequest<CloudBlobClient, CloudBlob, ArrayList<PageRange>>(
                options, this.getStorageUri()) {

            @Override
            public void setRequestLocationMode() {
                this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
            }

            @Override
            public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                    throws Exception {
                return BlobRequest.getPageRanges(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                        options, context, accessCondition, blob.snapshotID, offset, length);
            }

            @Override
            public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                    throws Exception {
                StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, context);
            }

            @Override
            public ArrayList<PageRange> preProcessResponse(CloudBlob parentObject, CloudBlobClient client,
                    OperationContext context) throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                    this.setNonExceptionedRetryableFailure(true);
                }

                return null;
            }

            @Override
            public ArrayList<PageRange> postProcessResponse(HttpURLConnection connection, CloudBlob blob,
                    CloudBlobClient client, OperationContext context, ArrayList<PageRange> storageObject)
                    throws Exception {
                blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                blob.updateLengthFromResponse(this.getConnection());

                return PageRangeHandler.getPageRanges(this.getConnection().getInputStream());
            }

        };

        return getRequest;
    }
    
    /**
     * Gets the collection of page ranges that differ between a specified snapshot and this object.
     * 
     * @param previousSnapshot
     *            A string representing the snapshot to use as the starting point for the diff. If this
     *            CloudPageBlob represents a snapshot, the previousSnapshot parameter must be prior to the current
     *            snapshot.
     * @return A <code>List</code> object containing the set of differing page ranges.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public List<PageRangeDiff> downloadPageRangesDiff(final String previousSnapshot) throws StorageException {
        return this.downloadPageRangesDiff(previousSnapshot, null /* offset */, null/* length */,
                null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Gets the collection of page ranges that differ between a specified snapshot and this object.
     * 
     * @param previousSnapshot
     *            A string representing the snapshot timestamp to use as the starting point for the diff. If this
     *            CloudPageBlob represents a snapshot, the previousSnapshot parameter must be prior to the current
     *            snapshot.
     * @param offset
     *            The starting offset of the data range over which to list page ranges, in bytes. Must be a multiple of
     *            512.
     * @param length
     *            The length of the data range over which to list page ranges, in bytes. Must be a multiple of 512.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return A <code>List</code> object containing the set of differing page ranges.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public List<PageRangeDiff> downloadPageRangesDiff(final String previousSnapshot, final Long offset,
            final Long length, final AccessCondition accessCondition, BlobRequestOptions options,
            OperationContext opContext) throws StorageException {

        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);

        return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                this.downloadPageRangesDiffImpl(previousSnapshot, offset, length, accessCondition, options),
                options.getRetryPolicyFactory(), opContext);
    }

    private StorageRequest<CloudBlobClient, CloudBlob, List<PageRangeDiff>> downloadPageRangesDiffImpl(
            final String previousSnapshot, final Long offset, final Long length, final AccessCondition accessCondition,
            final BlobRequestOptions options) {
        final StorageRequest<CloudBlobClient, CloudBlob, List<PageRangeDiff>> getRequest = new StorageRequest<CloudBlobClient, CloudBlob, List<PageRangeDiff>>(
                options, this.getStorageUri()) {

            @Override
            public void setRequestLocationMode() {
                this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
            }

            @Override
            public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                    throws Exception {
                return BlobRequest.getPageRangesDiff(
                        blob.getTransformedAddress(context).getUri(this.getCurrentLocation()), options, context,
                        accessCondition, blob.snapshotID, previousSnapshot, offset, length);
            }

            @Override
            public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                    throws Exception {
                StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, context);
            }

            @Override
            public List<PageRangeDiff> preProcessResponse(CloudBlob parentObject, CloudBlobClient client,
                    OperationContext context) throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                    this.setNonExceptionedRetryableFailure(true);
                }

                return null;
            }

            @Override
            public List<PageRangeDiff> postProcessResponse(HttpURLConnection connection, CloudBlob blob,
                    CloudBlobClient client, OperationContext context, List<PageRangeDiff> storageObject)
                    throws Exception {
                blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                blob.updateLengthFromResponse(this.getConnection());

                return PageRangeDiffHandler.getPageRangesDiff(this.getConnection().getInputStream());
            }
        };

        return getRequest;
    }

    /**
     * Opens an output stream object to write data to the page blob. The page blob must already exist and any existing 
     * data may be overwritten.
     * 
     * @return A {@link BlobOutputStream} object used to write data to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public BlobOutputStream openWriteExisting() throws StorageException {
        return this
                .openOutputStreamInternal(null /* length */, null /* premiumBlobTier */,null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Opens an output stream object to write data to the page blob, using the specified lease ID, request options and
     * operation context. The page blob must already exist and any existing data may be overwritten.
     * 
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return A {@link BlobOutputStream} object used to write data to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public BlobOutputStream openWriteExisting(AccessCondition accessCondition, BlobRequestOptions options,
            OperationContext opContext) throws StorageException {
        return this.openOutputStreamInternal(null /* length */, null /* premiumBlobTier */, accessCondition, options, opContext);
    }

    /**
     * Opens an output stream object to write data to the page blob. The page blob does not need to yet exist and will
     * be created with the length specified. If the blob already exists on the service, it will be overwritten.
     * <p>
     * To avoid overwriting and instead throw an error, please use the 
     * {@link #openWriteNew(long, AccessCondition, BlobRequestOptions, OperationContext)} overload with the appropriate 
     * {@link AccessCondition}.
     * 
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream to create. This value must be
     *            a multiple of 512.
     * 
     * @return A {@link BlobOutputStream} object used to write data to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public BlobOutputStream openWriteNew(final long length) throws StorageException {
        return this
                .openOutputStreamInternal(length, null /* premiumBlobTier */, null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Opens an output stream object to write data to the page blob, using the specified lease ID, request options and
     * operation context. The page blob does not need to yet exist and will be created with the length specified.If the
     * blob already exists on the service, it will be overwritten.
     * <p>
     * To avoid overwriting and instead throw an error, please pass in an {@link AccessCondition} generated using
     * {@link AccessCondition#generateIfNotExistsCondition()}.
     *
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream to create. This value must be
     *            a multiple of 512.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @return A {@link BlobOutputStream} object used to write data to the blob.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public BlobOutputStream openWriteNew(final long length, AccessCondition accessCondition,
                                         BlobRequestOptions options, OperationContext opContext) throws StorageException {
        return openOutputStreamInternal(length, null /* premiumBlobTier */, accessCondition, options, opContext);
    }

    /**
     * Opens an output stream object to write data to the page blob, using the specified lease ID, request options and
     * operation context. The page blob does not need to yet exist and will be created with the length specified.If the 
     * blob already exists on the service, it will be overwritten.
     * <p>
     * To avoid overwriting and instead throw an error, please pass in an {@link AccessCondition} generated using 
     * {@link AccessCondition#generateIfNotExistsCondition()}.
     * 
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream to create. This value must be
     *            a multiple of 512.
     * @param premiumBlobTier
     *            A {@link PremiumPageBlobTier} object which represents the tier of the blob.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return A {@link BlobOutputStream} object used to write data to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public BlobOutputStream openWriteNew(final long length, final PremiumPageBlobTier premiumBlobTier, AccessCondition accessCondition,
            BlobRequestOptions options, OperationContext opContext) throws StorageException {
        return openOutputStreamInternal(length, premiumBlobTier, accessCondition, options, opContext);
    }

    /**
     * Opens an output stream object to write data to the page blob, using the specified lease ID, request options and
     * operation context. If the length is specified, a new page blob will be created with the length specified.
     * Otherwise, the page blob must already exist and a stream of its current length will be opened.
     * 
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream to create. This value must be
     *            a multiple of 512 or null if the
     *            page blob already exists.
     * @param premiumBlobTier
     *            A {@link PremiumPageBlobTier} object which represents the tier of the blob.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return A {@link BlobOutputStream} object used to write data to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    private BlobOutputStream openOutputStreamInternal(Long length, PremiumPageBlobTier premiumBlobTier, AccessCondition accessCondition,
            BlobRequestOptions options, OperationContext opContext) throws StorageException {
        if (opContext == null) {
            opContext = new OperationContext();
        }

        assertNoWriteOperationForSnapshot();

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient,
                false /* setStartTime */);

        options.assertPolicyIfRequired();

        if (options.getStoreBlobContentMD5()) {
            throw new IllegalArgumentException(SR.BLOB_MD5_NOT_SUPPORTED_FOR_PAGE_BLOBS);
        }

        Cipher cipher = null;
        if (options.getEncryptionPolicy() != null) {
            cipher = options.getEncryptionPolicy().createAndSetEncryptionContext(this.getMetadata(), true /* noPadding */);
        }

        if (length != null) {
            if (length % Constants.PAGE_SIZE != 0) {
                throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
            }

            this.create(length, premiumBlobTier, accessCondition, options, opContext);
        }
        else {
            if (options.getEncryptionPolicy() != null) {
                throw new IllegalArgumentException(SR.ENCRYPTION_NOT_SUPPORTED_FOR_EXISTING_BLOBS);
            }
            this.downloadAttributes(accessCondition, options, opContext);
            length = this.getProperties().getLength();
        }

        if (accessCondition != null) {
            accessCondition = AccessCondition.generateLeaseCondition(accessCondition.getLeaseID());
        }

        if (options.getEncryptionPolicy() != null) {
            return new BlobEncryptStream(this, length, accessCondition, options, opContext, cipher);
        }
        else {
            return new BlobOutputStreamInternal(this, length, accessCondition, options, opContext);
        }
    }

    /**
     * Used for both uploadPages and clearPages.
     * 
     * @param pageRange
     *            A {@link PageRange} object that specifies the page range.
     * @param operationType
     *            A {@link PageOperationType} enumeration value that specifies the page operation type.
     * @param data
     *            A <code>byte</code> array which represents the data to write.
     * @param length
     *            A <code>long</code> which represents the number of bytes to write.
     * @param md5
     *            A <code>String</code> which represents the MD5 hash for the data.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    private void putPagesInternal(final PageRange pageRange, final PageOperationType operationType, final byte[] data,
            final long length, final String md5, final AccessCondition accessCondition,
            final BlobRequestOptions options, final OperationContext opContext) throws StorageException {
        ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                putPagesImpl(pageRange, operationType, data, length, md5, accessCondition, options, opContext),
                options.getRetryPolicyFactory(), opContext);
    }

    private StorageRequest<CloudBlobClient, CloudPageBlob, Void> putPagesImpl(final PageRange pageRange,
            final PageOperationType operationType, final byte[] data, final long length, final String md5,
            final AccessCondition accessCondition, final BlobRequestOptions options, final OperationContext opContext) {
        final StorageRequest<CloudBlobClient, CloudPageBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudPageBlob, Void>(
                options, this.getStorageUri()) {

            @Override
            public HttpURLConnection buildRequest(CloudBlobClient client, CloudPageBlob blob, OperationContext context)
                    throws Exception {
                if (operationType == PageOperationType.UPDATE) {
                    this.setSendStream(new ByteArrayInputStream(data));
                    this.setLength(length);
                }

                return BlobRequest.putPage(blob.getTransformedAddress(opContext).getUri(this.getCurrentLocation()),
                        options, opContext, accessCondition, pageRange, operationType);
            }

            @Override
            public void setHeaders(HttpURLConnection connection, CloudPageBlob blob, OperationContext context) {
                if (operationType == PageOperationType.UPDATE) {
                    if (options.getUseTransactionalContentMD5()) {
                        connection.setRequestProperty(Constants.HeaderConstants.CONTENT_MD5, md5);
                    }
                }
            }

            @Override
            public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                    throws Exception {
                if (operationType == PageOperationType.UPDATE) {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, length, context);
                }
                else {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, context);
                }
            }

            @Override
            public Void preProcessResponse(CloudPageBlob blob, CloudBlobClient client, OperationContext context)
                    throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                    this.setNonExceptionedRetryableFailure(true);
                    return null;
                }

                blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                blob.updateSequenceNumberFromResponse(this.getConnection());
                this.getResult().setRequestServiceEncrypted(BaseResponse.isServerRequestEncrypted(this.getConnection()));
                return null;
            }
        };

        return putRequest;
    }
    
    protected void updateSequenceNumberFromResponse(HttpURLConnection request) {
        final String sequenceNumber = request.getHeaderField(Constants.HeaderConstants.BLOB_SEQUENCE_NUMBER);
        if (!Utility.isNullOrEmpty(sequenceNumber)) {
            this.getProperties().setPageBlobSequenceNumber(Long.parseLong(sequenceNumber));
        }
    }

    /**
     * Resizes the page blob to the specified size.
     * 
     * @param size
     *            A <code>long</code> which represents the size of the page blob, in bytes.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public void resize(long size) throws StorageException {
        this.resize(size, null /* accessCondition */, null /* options */, null /* operationContext */);
    }

    /**
     * Resizes the page blob to the specified size.
     * 
     * @param size
     *            A <code>long</code> which represents the size of the page blob, in bytes.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public void resize(long size, AccessCondition accessCondition, BlobRequestOptions options,
            OperationContext opContext) throws StorageException {
        assertNoWriteOperationForSnapshot();

        if (size % Constants.PAGE_SIZE != 0) {
            throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
        }

        if (opContext == null) {
            opContext = new OperationContext();
        }

        opContext.initialize();
        options = BlobRequestOptions.populateAndApplyDefaults(options, this.properties.getBlobType(), this.blobServiceClient);

        ExecutionEngine.executeWithRetry(this.blobServiceClient, this, this.resizeImpl(size, accessCondition, options),
                options.getRetryPolicyFactory(), opContext);
    }

    private StorageRequest<CloudBlobClient, CloudPageBlob, Void> resizeImpl(final long size,
            final AccessCondition accessCondition, final BlobRequestOptions options) {
        final StorageRequest<CloudBlobClient, CloudPageBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudPageBlob, Void>(
                options, this.getStorageUri()) {

            @Override
            public HttpURLConnection buildRequest(CloudBlobClient client, CloudPageBlob blob, OperationContext context)
                    throws Exception {
                return BlobRequest.resize(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                        options, context, accessCondition, size);
            }

            @Override
            public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                    throws Exception {
                StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, context);
            }

            @Override
            public Void preProcessResponse(CloudPageBlob blob, CloudBlobClient client, OperationContext context)
                    throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                    this.setNonExceptionedRetryableFailure(true);
                    return null;
                }

                blob.getProperties().setLength(size);
                blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                blob.updateSequenceNumberFromResponse(this.getConnection());
                return null;
            }
        };

        return putRequest;
    }

    /**
     * Uploads a blob from data in a byte array. If the blob already exists on the service, it will be overwritten.
     *
     * @param buffer
     *            A <code>byte</code> array which represents the data to write to the blob.
     * @param offset
     *            A <code>int</code> which represents the offset of the byte array from which to start the data upload.
     * @param length
     *            An <code>int</code> which represents the number of bytes to upload from the input buffer.
     * @param premiumBlobTier
     *            A {@link PremiumPageBlobTier} object which represents the tier of the blob.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws IOException
     */
    public void uploadFromByteArray(final byte[] buffer, final int offset, final int length, final PremiumPageBlobTier premiumBlobTier,
                                    final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
            throws StorageException, IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(buffer, offset, length);
        this.upload(inputStream, length, premiumBlobTier, accessCondition, options, opContext);
        inputStream.close();
    }

    /**
     * Uploads a blob from a file. If the blob already exists on the service, it will be overwritten.
     *
     * @param path
     *            A <code>String</code> which represents the path to the file to be uploaded.
     * @param premiumBlobTier
     *            A {@link PremiumPageBlobTier} object which represents the tier of the blob.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws IOException
     */
    public void uploadFromFile(final String path, final PremiumPageBlobTier premiumBlobTier, final AccessCondition accessCondition, BlobRequestOptions options,
                               OperationContext opContext) throws StorageException, IOException {
        File file = new File(path);
        long fileLength = file.length();
        InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
        this.upload(inputStream, fileLength, premiumBlobTier, accessCondition, options, opContext);
        inputStream.close();
    }

    /**
     * Uploads the source stream data to the page blob. If the blob already exists on the service, it will be 
     * overwritten.
     * 
     * @param sourceStream
     *            An {@link InputStream} object to read from.
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream data, must be non zero and a
     *            multiple of 512.
     * 
     * @throws IOException
     *             If an I/O exception occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @Override
    @DoesServiceRequest
    public void upload(final InputStream sourceStream, final long length) throws StorageException, IOException {
        this.upload(sourceStream, length, null /* premiumBlobTier */, null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Uploads the source stream data to the page blob using the specified lease ID, request options, and operation
     * context. If the blob already exists on the service, it will be overwritten.
     *
     * @param sourceStream
     *            An {@link InputStream} object to read from.
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream data. This must be great than
     *            zero and a multiple of 512.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @throws IOException
     *             If an I/O exception occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @Override
    @DoesServiceRequest
    public void upload(final InputStream sourceStream, final long length, final AccessCondition accessCondition,
                       BlobRequestOptions options, OperationContext opContext) throws StorageException, IOException {
        this.upload(sourceStream, length, null /* premiumBlobTier*/, accessCondition, options, opContext);
    }

    /**
     * Uploads the source stream data to the page blob using the specified lease ID, request options, and operation
     * context. If the blob already exists on the service, it will be overwritten.
     * 
     * @param sourceStream
     *            An {@link InputStream} object to read from.
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream data. This must be great than
     *            zero and a multiple of 512.
     * @param premiumBlobTier
     *            A {@link PremiumPageBlobTier} object which represents the tier of the blob.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws IOException
     *             If an I/O exception occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void upload(final InputStream sourceStream, final long length, final PremiumPageBlobTier premiumBlobTier, final AccessCondition accessCondition,
            BlobRequestOptions options, OperationContext opContext) throws StorageException, IOException {
        assertNoWriteOperationForSnapshot();

        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);
        options.assertPolicyIfRequired();

        if (length <= 0 || length % Constants.PAGE_SIZE != 0) {
            throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
        }

        if (options.getStoreBlobContentMD5()) {
            throw new IllegalArgumentException(SR.BLOB_MD5_NOT_SUPPORTED_FOR_PAGE_BLOBS);
        }

        if (sourceStream.markSupported()) {
            // Mark sourceStream for current position.
            sourceStream.mark(Constants.MAX_MARK_LENGTH);
        }

        final BlobOutputStream streamRef = this.openWriteNew(length, premiumBlobTier, accessCondition, options, opContext);
        try {
            streamRef.write(sourceStream, length);
        }
        finally {
            streamRef.close();
        }
    }

    /**
     * Uploads a range of contiguous pages, up to 4 MB in size, at the specified offset in the page blob.
     * 
     * @param sourceStream
     *            An {@link InputStream} object which represents the input stream to write to the page blob.
     * @param offset
     *            A <code>long</code> which represents the offset, in number of bytes, at which to begin writing the
     *            data. This value must be a multiple of 512.
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the data to write. This value must be a
     *            multiple of 512.
     * 
     * @throws IllegalArgumentException
     *             If the offset or length are not multiples of 512, or if the length is greater than 4 MB.
     * @throws IOException
     *             If an I/O exception occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void uploadPages(final InputStream sourceStream, final long offset, final long length)
            throws StorageException, IOException {
        this.uploadPages(sourceStream, offset, length, null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Uploads a range of contiguous pages, up to 4 MB in size, at the specified offset in the page blob, using the
     * specified lease ID, request options, and operation context.
     * 
     * @param sourceStream
     *            An {@link InputStream} object which represents the input stream to write to the page blob.
     * @param offset
     *            A <code>long</code> which represents the offset, in number of bytes, at which to begin writing the
     *            data. This value must be a multiple of
     *            512.
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the data to write. This value must be a
     *            multiple of 512.
     * @param accessCondition
     *            An {@link AccessCondition} object which represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws IllegalArgumentException
     *             If the offset or length are not multiples of 512, or if the length is greater than 4 MB.
     * @throws IOException
     *             If an I/O exception occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void uploadPages(final InputStream sourceStream, final long offset, final long length,
            final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
            throws StorageException, IOException {

        if (offset % Constants.PAGE_SIZE != 0) {
            throw new IllegalArgumentException(SR.INVALID_PAGE_START_OFFSET);
        }

        if (length == 0 || length % Constants.PAGE_SIZE != 0) {
            throw new IllegalArgumentException(SR.INVALID_PAGE_BLOB_LENGTH);
        }

        if (length > 4 * Constants.MB) {
            throw new IllegalArgumentException(SR.INVALID_MAX_WRITE_SIZE);
        }

        assertNoWriteOperationForSnapshot();

        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);
        
        // Assert no encryption policy as this is not supported for partial uploads
        options.assertNoEncryptionPolicyOrStrictMode();

        final PageRange pageRange = new PageRange(offset, offset + length - 1);
        final byte[] data = new byte[(int) length];
        String md5 = null;

        int count = 0;
        int total = 0;
        while (total < length) {
            count = sourceStream.read(data, total, (int) Math.min(length - total, Integer.MAX_VALUE));
            total += count;
        }

        if (options.getUseTransactionalContentMD5()) {
            try {
                final MessageDigest digest = MessageDigest.getInstance("MD5");
                digest.update(data, 0, data.length);
                md5 = Base64.encode(digest.digest());
            }
            catch (final NoSuchAlgorithmException e) {
                // This wont happen, throw fatal.
                throw Utility.generateNewUnexpectedStorageException(e);
            }
        }

        this.putPagesInternal(pageRange, PageOperationType.UPDATE, data, length, md5, accessCondition, options,
                opContext);
    }

    /**
     * Sets the number of bytes to buffer when writing to a {@link BlobOutputStream}.
     * 
     * @param streamWriteSizeInBytes
     *            An <code>int</code> which represents the maximum number of bytes to buffer when writing to a page blob
     *            stream. This value must be a
     *            multiple of 512 and
     *            less than or equal to 4 MB.
     * 
     * @throws IllegalArgumentException
     *             If <code>streamWriteSizeInBytes</code> is less than 512, greater than 4 MB, or not a multiple or 512.
     */
    @Override
    public void setStreamWriteSizeInBytes(final int streamWriteSizeInBytes) {
        if (streamWriteSizeInBytes > Constants.MAX_PAGE_WRITE_SIZE || streamWriteSizeInBytes < Constants.PAGE_SIZE
            || streamWriteSizeInBytes % Constants.PAGE_SIZE != 0) {
            throw new IllegalArgumentException("StreamWriteSizeInBytes");
        }

        this.streamWriteSizeInBytes = streamWriteSizeInBytes;
    }
    
    /**
     * Sets the blob tier on a page blob on a premium storage account.
     * @param premiumBlobTier
     *            A {@link PremiumPageBlobTier} object which represents the tier of the blob.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void uploadPremiumPageBlobTier(final PremiumPageBlobTier premiumBlobTier) throws StorageException {
        this.uploadPremiumPageBlobTier(premiumBlobTier, null /* options */, null /* opContext */);
    }

    /**
     * Sets the tier on a page blob on a premium storage account.
     * @param premiumBlobTier
     *            A {@link PremiumPageBlobTier} object which represents the tier of the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object which represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void uploadPremiumPageBlobTier(final PremiumPageBlobTier premiumBlobTier, BlobRequestOptions options,
            OperationContext opContext) throws StorageException {
        assertNoWriteOperationForSnapshot();
        Utility.assertNotNull("premiumBlobTier", premiumBlobTier);

        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.PAGE_BLOB, this.blobServiceClient);

        ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                this.uploadBlobTierImpl(premiumBlobTier.toString(), options), options.getRetryPolicyFactory(), opContext);
        this.properties.setPremiumPageBlobTier(premiumBlobTier);
        this.properties.setBlobTierInferred(false);
    }
}