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

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.core.*;
import com.microsoft.azure.storage.file.CloudFile;

import javax.crypto.Cipher;
import javax.xml.stream.XMLStreamException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Represents a blob that is uploaded as a set of blocks.
 */
public final class CloudBlockBlob extends CloudBlob {

    /**
     * Flag that indicates if the default streamWriteSize has been modified.
     */
    private boolean isStreamWriteSizeModified = false;

    /**
     * Creates an instance of the <code>CloudBlockBlob</code> class using the specified absolute URI.
     * 
     * @param blobAbsoluteUri
     *            A <code>java.net.URI</code> object that represents the absolute URI to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public CloudBlockBlob(final URI blobAbsoluteUri) throws StorageException {
        this(new StorageUri(blobAbsoluteUri));
    }

    /**
     * Creates an instance of the <code>CloudBlockBlob</code> class using the specified absolute StorageUri.
     * 
     * @param blobAbsoluteUri
     *            A {@link StorageUri} object that represents the absolute URI to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public CloudBlockBlob(final StorageUri blobAbsoluteUri) throws StorageException {
        this(blobAbsoluteUri, (StorageCredentials)null);
    }

    /**
     * Creates an instance of the <code>CloudBlockBlob</code> class by copying values from another cloud block blob.
     * 
     * @param otherBlob
     *            A <code>CloudBlockBlob</code> object that represents the block blob to copy.
     */
    public CloudBlockBlob(final CloudBlockBlob otherBlob) {
        super(otherBlob);
    }

    /**
     * Creates an instance of the <code>CloudBlockBlob</code> class using the specified absolute URI and credentials.
     * 
     * @param blobAbsoluteUri
     *            A <code>java.net.URI</code> object that represents the absolute URI to the blob.
     * @param credentials
     *            A {@link StorageCredentials} object used to authenticate access.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public CloudBlockBlob(final URI blobAbsoluteUri, final StorageCredentials credentials) throws StorageException {
        this(new StorageUri(blobAbsoluteUri), credentials);
    }

    /**
     * Creates an instance of the <code>CloudBlockBlob</code> class using the specified absolute StorageUri and credentials.
     * 
     * @param blobAbsoluteUri
     *            A {@link StorageUri} object that represents the absolute StorageUri to the blob.
     * @param credentials
     *            A {@link StorageCredentials} object used to authenticate access.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public CloudBlockBlob(final StorageUri blobAbsoluteUri, final StorageCredentials credentials) throws StorageException {
        this(blobAbsoluteUri, null /* snapshotID */, credentials);
    }

    /**
     * Creates an instance of the <code>CloudBlockBlob</code> class using the specified absolute URI, snapshot ID, and
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
    public CloudBlockBlob(final URI blobAbsoluteUri, final String snapshotID, final StorageCredentials credentials)
            throws StorageException {
        this(new StorageUri(blobAbsoluteUri), snapshotID, credentials);
    }

    /**
     * Creates an instance of the <code>CloudBlockBlob</code> class using the specified absolute StorageUri, snapshot
     * ID, and credentials.
     * 
     * @param blobAbsoluteUri
     *            A {@link StorageUri} object that represents the absolute StorageUri to the blob.
     * @param snapshotID
     *            A <code>String</code> that represents the snapshot version, if applicable.
     * @param credentials
     *            A {@link StorageCredentials} object used to authenticate access.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public CloudBlockBlob(final StorageUri blobAbsoluteUri, final String snapshotID, final StorageCredentials credentials)
            throws StorageException {
        super(BlobType.BLOCK_BLOB, blobAbsoluteUri, snapshotID, credentials);
    }
    
    /**
     * Creates an instance of the <code>CloudBlockBlob</code> class using the specified type, name, snapshot ID, and
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
    protected CloudBlockBlob(String blobName, String snapshotID, CloudBlobContainer container)
            throws URISyntaxException {
        super(BlobType.BLOCK_BLOB, blobName, snapshotID, container);
    }
    
    /**
     * Requests the service to start copying a block blob's contents, properties, and metadata to a new block blob.
     *
     * @param sourceBlob
     *            A <code>CloudBlockBlob</code> object that represents the source blob to copy.
     *
     * @return A <code>String</code> which represents the copy ID associated with the copy operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws URISyntaxException
     */
    @DoesServiceRequest
    public final String startCopy(final CloudBlockBlob sourceBlob) throws StorageException, URISyntaxException {
        return this.startCopy(sourceBlob, null /* contentMd5 */, false /* syncCopy */, null /* sourceAccessCondition */,
                null /* destinationAccessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Requests the service to start copying a block blob's contents, properties, and metadata to a new block blob,
     * using the specified access conditions, lease ID, request options, and operation context.
     *
     * @param sourceBlob
     *            A <code>CloudBlockBlob</code> object that represents the source blob to copy.
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
    public final String startCopy(final CloudBlockBlob sourceBlob, final AccessCondition sourceAccessCondition,
                                  final AccessCondition destinationAccessCondition, BlobRequestOptions options, OperationContext opContext)
            throws StorageException, URISyntaxException {
        Utility.assertNotNull("sourceBlob", sourceBlob);

        URI source = sourceBlob.getSnapshotQualifiedUri();
        if (sourceBlob.getServiceClient() != null && sourceBlob.getServiceClient().getCredentials() != null)
        {
            source = sourceBlob.getServiceClient().getCredentials().transformUri(sourceBlob.getSnapshotQualifiedUri());
        }

        return this.startCopy(sourceBlob, null /* contentMd5 */, false /* syncCopy */, sourceAccessCondition, destinationAccessCondition, options, opContext);
    }

    /**
     * Requests the service to start copying a block blob's contents, properties, and metadata to a new block blob,
     * using the specified access conditions, lease ID, request options, and operation context.
     *
     * @param sourceBlob
     *            A <code>CloudBlockBlob</code> object that represents the source blob to copy.
     * @param contentMd5
     *            An optional hash value used to ensure transactional integrity for the operation. May be
     *            <code>null</code> or empty.
     * @param syncCopy
     *            A <code>boolean</code> to enable synchronous server copy of blobs.
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
    private final String startCopy(final CloudBlockBlob sourceBlob, String contentMd5, boolean syncCopy, final AccessCondition sourceAccessCondition,
            final AccessCondition destinationAccessCondition, BlobRequestOptions options, OperationContext opContext)
            throws StorageException, URISyntaxException {
        Utility.assertNotNull("sourceBlob", sourceBlob);

        URI source = sourceBlob.getSnapshotQualifiedUri();
        if (sourceBlob.getServiceClient() != null && sourceBlob.getServiceClient().getCredentials() != null)
        {
            source = sourceBlob.getServiceClient().getCredentials().transformUri(sourceBlob.getSnapshotQualifiedUri());
        }

        return this.startCopy(source, null /* premiumPageBlobTier */, sourceAccessCondition, destinationAccessCondition, options, opContext);
    }

    /**
    * Requests the service to start copying a file's contents, properties, and metadata to a new block blob.
    *
    * @param sourceFile
    *             A <code>CloudFile</code> object that represents the source file to copy.
    *
    * @return A <code>String</code> which represents the copy ID associated with the copy operation.
    *
    * @throws StorageException
    *             If a storage service error occurred.
    * @throws URISyntaxException
    */
   @DoesServiceRequest
   public final String startCopy(final CloudFile sourceFile) throws StorageException, URISyntaxException {
       return this.startCopy(sourceFile, null /* sourceAccessCondition */,
               null /* destinationAccessCondition */, null /* options */, null /* opContext */);
   }

   /**
    * Requests the service to start copying a file's contents, properties, and metadata to a new block blob,
    * using the specified access conditions, lease ID, request options, and operation context.
    *
    * @param sourceFile
    *            A <code>CloudFile</code> object that represents the source file to copy.
    * @param sourceAccessCondition
    *            An {@link AccessCondition} object that represents the access conditions for the source file.
    * @param destinationAccessCondition
    *            An {@link AccessCondition} object that represents the access conditions for the destination block blob.
    * @param options
    *            A {@link BlobRequestOptions} object that specifies any additional options for the request.
    *            Specifying <code>null</code> will use the default request options from the associated
    *            service client ({@link CloudBlobClient}).
    * @param opContext
    *            An {@link OperationContext} object that represents the context for the current operation.
    *            This object is used to track requests to the storage service, and to provide additional
    *            runtime information about the operation.
    *
    * @return A <code>String</code> which represents the copy ID associated with the copy operation.
    *
    * @throws StorageException
    *             If a storage service error occurred.
    * @throws URISyntaxException
    *             If the resource URI is invalid.
    */
   @DoesServiceRequest
   public final String startCopy(final CloudFile sourceFile, final AccessCondition sourceAccessCondition,
           final AccessCondition destinationAccessCondition, BlobRequestOptions options, OperationContext opContext)
           throws StorageException, URISyntaxException {
       Utility.assertNotNull("sourceFile", sourceFile);
       return this.startCopy(
               sourceFile.getServiceClient().getCredentials().transformUri(sourceFile.getUri()),
               null /* premiumPageBlobTier */, sourceAccessCondition, destinationAccessCondition, options, opContext);
   }

    /**
     * Commits a block list to the storage service. In order to be written as part of a blob, a block must have been
     * successfully written to the server in a prior uploadBlock operation.
     * 
     * @param blockList
     *            An enumerable collection of {@link BlockEntry} objects that represents the list block items being
     *            committed. The <code>size</code> field is ignored.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void commitBlockList(final Iterable<BlockEntry> blockList) throws StorageException {
        this.commitBlockList(blockList, null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Commits a block list to the storage service using the specified lease ID, request options, and operation context.
     * In order to be written as part of a blob, a block must have been successfully written to the server in a prior
     * uploadBlock operation.
     * 
     * @param blockList
     *            An enumerable collection of {@link BlockEntry} objects that represents the list block items being
     *            committed. The size field is ignored.
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
     */
    @DoesServiceRequest
    public void commitBlockList(final Iterable<BlockEntry> blockList, final AccessCondition accessCondition,
            BlobRequestOptions options, OperationContext opContext) throws StorageException {
        assertNoWriteOperationForSnapshot();

        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient);
        
        ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                this.commitBlockListImpl(blockList, accessCondition, options, opContext),
                options.getRetryPolicyFactory(), opContext);
    }

    private StorageRequest<CloudBlobClient, CloudBlob, Void> commitBlockListImpl(final Iterable<BlockEntry> blockList,
            final AccessCondition accessCondition, final BlobRequestOptions options, final OperationContext opContext)
            throws StorageException {

        byte[] blockListBytes;
        try {
            blockListBytes = BlockEntryListSerializer.writeBlockListToStream(blockList, opContext);

            final ByteArrayInputStream blockListInputStream = new ByteArrayInputStream(blockListBytes);

            // This also marks the stream. Therefore no need to mark it in buildRequest.
            final StreamMd5AndLength descriptor = Utility.analyzeStream(blockListInputStream, -1L, -1L,
                    true /* rewindSourceStream */, options.getUseTransactionalContentMD5() /* calculateMD5 */);

            final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                        throws Exception {
                    this.setSendStream(blockListInputStream);
                    this.setLength(descriptor.getLength());
                    return BlobRequest.putBlockList(
                            blob.getTransformedAddress(context).getUri(this.getCurrentLocation()), options, context,
                            accessCondition, blob.properties);
                }

                @Override
                public void setHeaders(HttpURLConnection connection, CloudBlob blob, OperationContext context) {
                    BlobRequest.addMetadata(connection, blob.metadata, context);

                    if (options.getUseTransactionalContentMD5()) {
                        connection.setRequestProperty(Constants.HeaderConstants.CONTENT_MD5, descriptor.getMd5());
                    }
                }

                @Override
                public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                        throws Exception {
                    StorageRequest.signBlobQueueAndFileRequest(connection, client, this.getLength(), context);
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
                    return null;
                }

                @Override
                public void recoveryAction(OperationContext context) throws IOException {
                    blockListInputStream.reset();
                    blockListInputStream.mark(Constants.MAX_MARK_LENGTH);
                }
            };

            return putRequest;
        }
        catch (XMLStreamException e) {
            // The request was not even made. There was an error while trying to write the block list. Just throw.
            StorageException translatedException = StorageException.translateClientException(e);
            throw translatedException;
        }
        catch (IOException e) {
            // The request was not even made. There was an error while trying to write the block list. Just throw.
            StorageException translatedException = StorageException.translateClientException(e);
            throw translatedException;
        }
    }

    /**
     * Downloads the committed block list from the block blob.
     * <p>
     * The committed block list includes the list of blocks that have been successfully committed to the block blob. The
     * list of committed blocks is returned in the same order that they were committed to the blob. No block may appear
     * more than once in the committed block list.
     * 
     * @return An <code>ArrayList</code> object of {@link BlockEntry} objects that represent the committed list
     *         block items downloaded from the block blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public ArrayList<BlockEntry> downloadBlockList() throws StorageException {
        return this.downloadBlockList(BlockListingFilter.COMMITTED, null /* accessCondition */, null /* options */,
                null /* opContext */);
    }

    /**
     * Downloads the block list from the block blob using the specified block listing filter, request options, and
     * operation context.
     * <p>
     * The committed block list includes the list of blocks that have been successfully committed to the block blob. The
     * list of committed blocks is returned in the same order that they were committed to the blob. No block may appear
     * more than once in the committed block list.
     * 
     * @param blockListingFilter
     *            A {@link BlockListingFilter} value that specifies whether to download committed blocks, uncommitted
     *            blocks, or all blocks.
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
     * @return An <code>ArrayList</code> object of {@link BlockEntry} objects that represent the list block items
     *         downloaded from the block blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public ArrayList<BlockEntry> downloadBlockList(final BlockListingFilter blockListingFilter,
            final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
            throws StorageException {
        Utility.assertNotNull("blockListingFilter", blockListingFilter);

        if (opContext == null) {
            opContext = new OperationContext();
        }

        opContext.initialize();
        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient);

        return ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                this.downloadBlockListImpl(blockListingFilter, accessCondition, options),
                options.getRetryPolicyFactory(), opContext);
    }

    private StorageRequest<CloudBlobClient, CloudBlob, ArrayList<BlockEntry>> downloadBlockListImpl(
            final BlockListingFilter blockListingFilter, final AccessCondition accessCondition,
            final BlobRequestOptions options) {
        final StorageRequest<CloudBlobClient, CloudBlob, ArrayList<BlockEntry>> getRequest = new StorageRequest<CloudBlobClient, CloudBlob, ArrayList<BlockEntry>>(
                options, this.getStorageUri()) {

            @Override
            public void setRequestLocationMode() {
                this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
            }

            @Override
            public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                    throws Exception {
                return BlobRequest.getBlockList(blob.getTransformedAddress(context).getUri(this.getCurrentLocation()),
                        options, context, accessCondition, blob.snapshotID, blockListingFilter);
            }

            @Override
            public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                    throws Exception {
                StorageRequest.signBlobQueueAndFileRequest(connection, client, -1L, context);
            }

            @Override
            public ArrayList<BlockEntry> preProcessResponse(CloudBlob blob, CloudBlobClient client,
                    OperationContext context) throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                    this.setNonExceptionedRetryableFailure(true);
                }

                return null;
            }

            @Override
            public ArrayList<BlockEntry> postProcessResponse(HttpURLConnection connection, CloudBlob blob,
                    CloudBlobClient client, OperationContext context, ArrayList<BlockEntry> storageObject)
                    throws Exception {
                blob.updateEtagAndLastModifiedFromResponse(this.getConnection());
                blob.updateLengthFromResponse(this.getConnection());

                return BlockListHandler.getBlockList(this.getConnection().getInputStream());
            }
        };

        return getRequest;
    }

    /**
     * Creates and opens an output stream to write data to the block blob. If the blob already exists on the service, it
     * will be overwritten.
     * <p>
     * To avoid overwriting and instead throw an error, please use the 
     * {@link #openOutputStream(AccessCondition, BlobRequestOptions, OperationContext)} overload with the appropriate 
     * {@link AccessCondition}.
     * 
     * @return A {@link BlobOutputStream} object used to write data to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public BlobOutputStream openOutputStream() throws StorageException {
        return this.openOutputStream(null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Creates and opens an output stream to write data to the block blob using the specified request options and
     * operation context. If the blob already exists on the service, it will be overwritten.
     * <p>
     * To avoid overwriting and instead throw an error, please pass in an {@link AccessCondition} generated using 
     * {@link AccessCondition#generateIfNotExistsCondition()}.
     * 
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
     * @return A {@link BlobOutputStream} object used to write data to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public BlobOutputStream openOutputStream(AccessCondition accessCondition, BlobRequestOptions options,
            OperationContext opContext) throws StorageException {
        if (opContext == null) {
            opContext = new OperationContext();
        }

        assertNoWriteOperationForSnapshot();

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient, 
                false /* setStartTime */);
        options.assertPolicyIfRequired();
        
        // TODO: Apply any conditional access conditions up front

        if(options.getEncryptionPolicy() != null) {
            Cipher cipher = options.getEncryptionPolicy().createAndSetEncryptionContext(this.getMetadata(), false /* noPadding */);
            return new BlobEncryptStream(this, accessCondition, options, opContext, cipher);
        } else {
            return new BlobOutputStreamInternal(this, accessCondition, options, opContext);
        }
    }

    /**
     * Uploads the source stream data to the block blob. If the blob already exists on the service, it will be 
     * overwritten.
     * 
     * @param sourceStream
     *            An {@link InputStream} object that represents the input stream to write to the block blob.
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream data, or -1 if unknown.
     * 
     * @throws IOException
     *             If an I/O error occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @Override
    @DoesServiceRequest
    public void upload(final InputStream sourceStream, final long length) throws StorageException, IOException {
        this.upload(sourceStream, length, null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Uploads the source stream data to the blob, using the specified lease ID, request options, and operation context.
     * If the blob already exists on the service, it will be overwritten.
     * 
     * @param sourceStream
     *            An {@link InputStream} object that represents the input stream to write to the block blob.
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream data, or -1 if unknown.
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
     * @throws IOException
     *             If an I/O error occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @Override
    @DoesServiceRequest
    public void upload(final InputStream sourceStream, final long length, final AccessCondition accessCondition,
            BlobRequestOptions options, OperationContext opContext) throws StorageException, IOException {
        if (length < -1) {
            throw new IllegalArgumentException(SR.STREAM_LENGTH_NEGATIVE);
        }

        assertNoWriteOperationForSnapshot();

        if (opContext == null) {
            opContext = new OperationContext();
        }

        opContext.initialize();
        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient);
        options.assertPolicyIfRequired();

        StreamMd5AndLength descriptor = new StreamMd5AndLength();
        descriptor.setLength(length);

        // If the sourceStream is a FileInputStream, wrap it in a MarkableFileStream.
        // This allows for single shot upload on FileInputStreams.
        InputStream inputDataStream;
        if(!sourceStream.markSupported() && sourceStream instanceof FileInputStream) {
            inputDataStream = new MarkableFileStream((FileInputStream)sourceStream);
        }
        else {
            inputDataStream = sourceStream;
        }
        
        // Initial check - skip the PutBlob operation if the input stream isn't markable, or if the length is known to
        // be greater than the threshold.
        boolean skipPutBlob = !inputDataStream.markSupported() || descriptor.getLength() > options.getSingleBlobPutThresholdInBytes();

        if (inputDataStream.markSupported()) {
            // Mark sourceStream for current position.
            inputDataStream.mark(Constants.MAX_MARK_LENGTH);
        }

        // If we're not yet skipping PutBlob and we need to encrypt, encrypt the data and check that the encrypted
        // data is under the threshold.
        // Note this will abort at
        // options.getSingleBlobPutThresholdInBytes() bytes and return -1.        
        if (!skipPutBlob && options.getEncryptionPolicy() != null) {
            class GettableByteArrayOutputStream extends ByteArrayOutputStream {
                public byte[] getByteArray() {
                    return this.buf;
                }
            }
            
            Cipher cipher = options.getEncryptionPolicy().createAndSetEncryptionContext(this.getMetadata(), false /* noPadding */);
            GettableByteArrayOutputStream targetStream = new GettableByteArrayOutputStream();
            long byteCount = Utility.encryptStreamIfUnderThreshold(inputDataStream, targetStream, cipher, descriptor.getLength(),
                    options.getSingleBlobPutThresholdInBytes() + 1 /*abandon if the operation hits this limit*/);
            
            if (byteCount >= 0)
            {
                inputDataStream = new ByteArrayInputStream(targetStream.getByteArray());
                descriptor.setLength(byteCount);
            }
            else {
                // If the encrypted data is over the threshold, skip PutBlob.
                skipPutBlob = true;
            }
        }
        
        // If we're not yet skipping PutBlob, and the length is still unknown or we need to
        // set md5, then analyze the stream.
        // Note this read will abort at
        // options.getSingleBlobPutThresholdInBytes() bytes and return
        // -1 as length in which case we will revert to using a stream as it is
        // over the single put threshold.
        if (!skipPutBlob && (descriptor.getLength() < 0 || options.getStoreBlobContentMD5())) {
            // If the stream is of unknown length or we need to calculate
            // the MD5, then we we need to read the stream contents first

            descriptor = Utility.analyzeStream(inputDataStream, descriptor.getLength(), 
                    options.getSingleBlobPutThresholdInBytes() + 1 /*abandon if the operation hits this limit*/, 
                    true /* rewindSourceStream */, options.getStoreBlobContentMD5());

            if (descriptor.getMd5() != null && options.getStoreBlobContentMD5()) {
                this.properties.setContentMD5(descriptor.getMd5());
            }
            
            // If the data is over the threshold, skip PutBlob.  
            if (descriptor.getLength() == -1 || descriptor.getLength() > options.getSingleBlobPutThresholdInBytes())
            {
                skipPutBlob = true;
            }
        }

        // By now, the skipPutBlob is completely correct.
        if (!skipPutBlob) {
            this.uploadFullBlob(inputDataStream, descriptor.getLength(), accessCondition, options, opContext);
        }
        else {
            int totalBlocks = (int) Math.ceil((double) length / (double) this.streamWriteSizeInBytes);

            // Check if the upload will fail because the total blocks exceeded the maximum allowable limit.
            if (length != -1 && totalBlocks > Constants.MAX_BLOCK_NUMBER) {
                if (this.isStreamWriteSizeModified()) {
                    // User has set the block write size explicitly and will need to adjust it manually.
                    throw new IOException(SR.BLOB_OVER_MAX_BLOCK_LIMIT);
                }
                else {
                    // Scale so the upload succeeds (only if the block write size was not modified).
                    this.streamWriteSizeInBytes = (int) Math.ceil((double) length / (double) Constants.MAX_BLOCK_NUMBER);
                    totalBlocks = (int) Math.ceil((double) length / (double) this.streamWriteSizeInBytes);
                }
            }

            boolean useOpenWrite = options.getEncryptionPolicy() != null
                                   || !inputDataStream.markSupported()
                                   || this.streamWriteSizeInBytes < Constants.MIN_LARGE_BLOCK_SIZE
                                   || options.getStoreBlobContentMD5()
                                   || descriptor.getLength() == -1;

            // there are two known issues with the uploadFromMultiStream logic
            // 1. The same block ids are being used for each batch of uploads.
            // 2. When using a bufferedInputStream and the size of the stream being uploaded exceeds Integer.MAX_VALUE,
            //    a NegativeArraySizeException is thrown when attempting to skip past the 1 GB mark.
            useOpenWrite = true;
            if (useOpenWrite) {
                final BlobOutputStream writeStream = this.openOutputStream(accessCondition, options, opContext);
                try {
                    writeStream.write(inputDataStream, length);
                }
                finally {
                    writeStream.close();
                }
            }
            else {
                int blocksAllowedPerBatch = Integer.MAX_VALUE / this.streamWriteSizeInBytes;
                List<BlockEntry> blockList = new ArrayList<BlockEntry>();

                while (totalBlocks > 0) {
                    sourceStream.mark(Integer.MAX_VALUE);
                    int blocksInBatch = Math.min(totalBlocks, blocksAllowedPerBatch);
                    SubStreamGenerator subStreamGenerator = new SubStreamGenerator(sourceStream, blocksInBatch, this.streamWriteSizeInBytes);
                    if (totalBlocks == blocksInBatch && (length % this.streamWriteSizeInBytes != 0)) {
                        subStreamGenerator.setLastBlockSize(length % this.streamWriteSizeInBytes);
                    }

                    totalBlocks -= blocksInBatch;
                    this.uploadFromMultiStream(subStreamGenerator, accessCondition, options, opContext, blockList);
                }

                this.commitBlockList(blockList,accessCondition, options, opContext);
            }
        }
    }

    /**
     * Uploads an Iterable collection of streams using PutBlock.
     *
     * @param streamList
     *            An <code>Iterable</code> object of type <code>InputStream</code> that represent the
     *            source streams to be uploaded.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the blob.
     * @param requestOptions
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param operationContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param blockList
     *            An List object of type (@link BlockEntry} that will be populated with the BlockEntry for each uploaded block.
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws IOException
     *             If an I/O error occurred.
     */
    private void uploadFromMultiStream(Iterable<InputStream> streamList, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext operationContext, List<BlockEntry> blockList)
            throws StorageException, IOException {
        int concurrentOpsCount = requestOptions.getConcurrentRequestCount();

        // The ExecutorService used to schedule putblock tasks for this stream.
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                concurrentOpsCount,
                concurrentOpsCount,
                10,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());

        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(threadPool);
        final BlobRequestOptions _requestOptions = requestOptions;
        final OperationContext _operationContext = operationContext;
        final AccessCondition _accessCondition = accessCondition;
        int blockNum = 0;
        for (InputStream block : streamList) {

            // Pad block id up to digits in MaxBlockSize
            final String blockId = Base64.encode(
                    String.format("Block_%05d", ++blockNum).getBytes()
            );

            BlockEntry blockEntry = new BlockEntry(blockId);
            blockList.add(blockEntry);
            final InputStream sourceStream = block;
            final long blockSize = block instanceof SubStream ? ((SubStream) block).getLength() : this.streamWriteSizeInBytes;

            completionService.submit(new Callable<Void>() {
                @Override
                public Void call() throws IOException, StorageException {
                    uploadBlock(blockId, sourceStream, blockSize, _accessCondition, _requestOptions, _operationContext);
                    sourceStream.close();
                    return null;
                }
            });
        }

        for (int i = 0; i < blockNum; i++) {
            this.waitAny(completionService);
        }

        try {
            // Shutdown the thread pool executor.
            threadPool.shutdown();
        }
        finally {
            if (!threadPool.isShutdown()) {
                threadPool.shutdownNow();
            }
        }
    }

    /**
     * Uploads a blob in a single operation.
     *
     * @param sourceStream
     *            A <code>InputStream</code> object that represents the source stream to upload.
     * @param length
     *            The length, in bytes, of the stream, or -1 if unknown.
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
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    protected final void uploadFullBlob(final InputStream sourceStream, final long length,
            final AccessCondition accessCondition, final BlobRequestOptions options, final OperationContext opContext)
            throws StorageException {
        assertNoWriteOperationForSnapshot();

        // Mark sourceStream for current position.
        sourceStream.mark(Constants.MAX_MARK_LENGTH);

        if (length < 0 || length > BlobConstants.MAX_SINGLE_UPLOAD_BLOB_SIZE_IN_BYTES) {
            throw new IllegalArgumentException(String.format(SR.INVALID_STREAM_LENGTH,
                    BlobConstants.MAX_SINGLE_UPLOAD_BLOB_SIZE_IN_BYTES / Constants.MB));
        }

        ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                uploadFullBlobImpl(sourceStream, length, accessCondition, options, opContext),
                options.getRetryPolicyFactory(), opContext);
    }

    private StorageRequest<CloudBlobClient, CloudBlob, Void> uploadFullBlobImpl(final InputStream sourceStream,
            final long length, final AccessCondition accessCondition, final BlobRequestOptions options,
            final OperationContext opContext) {
        final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                options, this.getStorageUri()) {

            @Override
            public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                    throws Exception {
                this.setSendStream(sourceStream);
                this.setLength(length);
                return BlobRequest.putBlob(blob.getTransformedAddress(opContext).getUri(this.getCurrentLocation()),
                        options, opContext, accessCondition, blob.properties, blob.properties.getBlobType(),
                        this.getLength());
            }

            @Override
            public void setHeaders(HttpURLConnection connection, CloudBlob blob, OperationContext context) {
                BlobRequest.addMetadata(connection, blob.metadata, opContext);
            }

            @Override
            public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                    throws Exception {
                StorageRequest.signBlobQueueAndFileRequest(connection, client, length, context);
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
                return null;
            }

            @Override
            public void recoveryAction(OperationContext context) throws IOException {
                sourceStream.reset();
                sourceStream.mark(Constants.MAX_MARK_LENGTH);
            }

            @Override
            public void validateStreamWrite(StreamMd5AndLength descriptor) throws StorageException {
                if (this.getLength() != null && this.getLength() != -1) {
                    if (length != descriptor.getLength()) {
                        throw new StorageException(StorageErrorCodeStrings.INVALID_INPUT, SR.INCORRECT_STREAM_LENGTH,
                                HttpURLConnection.HTTP_FORBIDDEN, null, null);
                    }
                }
            }
        };

        return putRequest;
    }
    
    /**
     * Uploads a block to be committed as part of the block blob, using the specified block ID.
     * 
     * @param blockId
     *            A <code>String</code> that represents the Base-64 encoded block ID. Note for a given blob the length
     *            of all Block IDs must be identical.
     * @param sourceStream
     *            An {@link InputStream} object that represents the input stream to write to the block blob.
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream data, or -1 if unknown.
     * @throws IOException
     *             If an I/O error occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void uploadBlock(final String blockId, final InputStream sourceStream, final long length)
            throws StorageException, IOException {
        this.uploadBlock(blockId, sourceStream, length, null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Uploads a block to be committed as part of the block blob, using the specified block ID, the specified lease ID,
     * request options, and operation context.
     * 
     * @param blockId
     *            A <code>String</code> that represents the Base-64 encoded block ID. Note for a given blob the length
     *            of all Block IDs must be identical.
     * @param sourceStream
     *            An {@link InputStream} object that represents the input stream to write to the block blob.
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream data, or -1 if unknown.
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
     * @throws IOException
     *             If an I/O error occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void uploadBlock(final String blockId, final InputStream sourceStream, final long length,
            final AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext)
            throws StorageException, IOException {

        if (length < -1) {
            throw new IllegalArgumentException(SR.STREAM_LENGTH_NEGATIVE);
        }

        assertNoWriteOperationForSnapshot();

        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient);

        // Assert no encryption policy as this is not supported for partial uploads
        options.assertNoEncryptionPolicyOrStrictMode();

        // Assert block length
        if (Utility.isNullOrEmpty(blockId) || !Base64.validateIsBase64String(blockId)) {
            throw new IllegalArgumentException(SR.INVALID_BLOCK_ID);
        }

        // If the sourceStream is a FileInputStream, wrap it in a MarkableFileStream.
        // This prevents buffering the entire block into memory.
        InputStream bufferedStreamReference;
        if(!sourceStream.markSupported() && sourceStream instanceof FileInputStream) {
            bufferedStreamReference = new MarkableFileStream((FileInputStream)sourceStream);
        }
        else {
            bufferedStreamReference = sourceStream;
        }

        if (bufferedStreamReference.markSupported()) {
            // Mark sourceStream for current position.
            bufferedStreamReference.mark(Constants.MAX_MARK_LENGTH);
        }

        StreamMd5AndLength descriptor = new StreamMd5AndLength();
        descriptor.setLength(length);

        if (!bufferedStreamReference.markSupported()) {
            // needs buffering
            // TODO: Change to a BufferedInputStream to avoid the extra buffering and copying.
            final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            descriptor = Utility.writeToOutputStream(bufferedStreamReference, byteStream, length, false /* rewindSourceStream */,
                    options.getUseTransactionalContentMD5(), opContext, options);

            bufferedStreamReference = new ByteArrayInputStream(byteStream.toByteArray());
        }
        else if (length < 0 || options.getUseTransactionalContentMD5()) {
            // If the stream is of unknown length or we need to calculate the
            // MD5, then we we need to read the stream contents first
            descriptor = Utility.analyzeStream(bufferedStreamReference, length, -1L, true /* rewindSourceStream */,
                    options.getUseTransactionalContentMD5());
        }

        if (descriptor.getLength() > Constants.MAX_BLOCK_SIZE)
        {
            throw new IllegalArgumentException(SR.STREAM_LENGTH_GREATER_THAN_100MB);
        }

        this.uploadBlockInternal(blockId, descriptor.getMd5(), bufferedStreamReference, descriptor.getLength(),
                accessCondition, options, opContext);
    }

    /**
     * Uploads a block of the blob to the server.
     * 
     * @param blockId
     *            A <code>String</code> which represents the Base64-encoded Block ID.
     * @param md5
     *            A <code>String</code> which represents the MD5 to use if it is set.
     * @param sourceStream
     *            An {@link InputStream} object to read from.
     * @param length
     *            A <code>long</code> which represents the length, in bytes, of the stream data, or -1 if unknown.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request.
     * @param opContext
     *            An {@link OperationContext} object that is used to track the execution of the operation.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    private void uploadBlockInternal(final String blockId, final String md5, final InputStream sourceStream,
            final long length, final AccessCondition accessCondition, final BlobRequestOptions options,
            final OperationContext opContext) throws StorageException {
        ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                uploadBlockImpl(blockId, md5, sourceStream, length, accessCondition, options, opContext),
                options.getRetryPolicyFactory(), opContext);
    }

    private StorageRequest<CloudBlobClient, CloudBlob, Void> uploadBlockImpl(final String blockId, final String md5,
            final InputStream sourceStream, final long length, final AccessCondition accessCondition,
            final BlobRequestOptions options, final OperationContext opContext) {

        final StorageRequest<CloudBlobClient, CloudBlob, Void> putRequest = new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                options, this.getStorageUri()) {

            @Override
            public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                    throws Exception {
                this.setSendStream(sourceStream);
                this.setLength(length);
                return BlobRequest.putBlock(blob.getTransformedAddress(opContext).getUri(this.getCurrentLocation()),
                        options, opContext, accessCondition, blockId);
            }

            @Override
            public void setHeaders(HttpURLConnection connection, CloudBlob blob, OperationContext context) {
                if (options.getUseTransactionalContentMD5()) {
                    connection.setRequestProperty(Constants.HeaderConstants.CONTENT_MD5, md5);
                }
            }

            @Override
            public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                    throws Exception {
                StorageRequest.signBlobQueueAndFileRequest(connection, client, length, context);
            }

            @Override
            public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                    throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                    this.setNonExceptionedRetryableFailure(true);
                    return null;
                }

                this.getResult().setRequestServiceEncrypted(BaseResponse.isServerRequestEncrypted(this.getConnection()));
                return null;
            }

            @Override
            public void recoveryAction(OperationContext context) throws IOException {
                sourceStream.reset();
                sourceStream.mark(Constants.MAX_MARK_LENGTH);
            }
        };

        return putRequest;
    }

    /**
     * Creates a block to be committed as part of the block blob, using the specified block ID and the source URL.
     *
     * @param blockId
     *            A <code>String</code> that represents the Base-64 encoded block ID. Note for a given blob the length
     *            of all Block IDs must be identical.
     * @param copySource
     *            The <code>URI</code> of the source data. It can point to any Azure Blob or File that is public or the
     *            URL can include a shared access signature.
     * @param offset
     *           A <code>long</code> which represents the offset to use as the starting point for the source.
     * @param length
     *           A <code>Long</code> which represents the number of bytes to copy or <code>null</code> to copy until the
     *           end of the blob.
     * @throws IOException
     *             If an I/O error occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void createBlockFromURI(final String blockId, final URI copySource, final Long offset,
                                   final Long length)
            throws StorageException, IOException {
        this.createBlockFromURI(blockId, copySource, offset, length, null /* md5 */,
                null /* accessCondition */,null /* options */, null /* opContext */);
    }

    /**
     * Creates a block to be committed as part of the block blob, using the specified block ID, the specified source
     * URL, the specified lease ID, request options, and operation context.
     *
     * @param blockId
     *            A <code>String</code> that represents the Base-64 encoded block ID. Note for a given blob the length
     *            of all Block IDs must be identical.
     * @param copySource
     *            The <code>URI</code> of the source data. It can point to any Azure Blob or File that is public or the
     *            URL can include a shared access signature.
     * @param offset
     *           A <code>long</code> which represents the offset to use as the starting point for the source.
     * @param length
     *           A <code>Long</code> which represents the number of bytes to copy or <code>null</code> to copy until the
     *           end of the blob.
     * @param md5
     *           A <code>String</code> which represents the MD5 caluclated for the range of bytes of the source.
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
     */
    @DoesServiceRequest
    public void createBlockFromURI(final String blockId, final URI copySource, final Long offset, final Long length,
                                   String md5, final AccessCondition accessCondition, BlobRequestOptions options,
                                   OperationContext opContext)
            throws StorageException {

        Utility.assertNotNull("copySource", copySource);

        assertNoWriteOperationForSnapshot();

        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient);

        // Assert no encryption policy as this is not supported for partial uploads
        options.assertNoEncryptionPolicyOrStrictMode();

        // Assert block length
        if (Utility.isNullOrEmpty(blockId) || !Base64.validateIsBase64String(blockId)) {
            throw new IllegalArgumentException(SR.INVALID_BLOCK_ID);
        }

        if (length != null && length > Constants.MAX_BLOCK_SIZE)
        {
            throw new IllegalArgumentException(SR.COPY_SIZE_GREATER_THAN_100MB);
        }

        this.createBlockFromURIInternal(blockId, copySource, offset, length, md5,
                accessCondition, options, opContext);
    }

    /**
     * Creates a block to be committed as part of the block blob, using the specified block ID, the specified source
     * URL, the specified lease ID, request options, and operation context.
     *
     * @param blockId
     *            A <code>String</code> that represents the Base-64 encoded block ID. Note for a given blob the length
     *            of all Block IDs must be identical.
     * @param copySource
     *            The <code>URI</code> of the source data. It can point to any Azure Blob or File that is public or the
     *            URL can include a shared access signature.
     * @param offset
     *           A <code>long</code> which represents the offset to use as the starting point for the source.
     * @param length
     *           A <code>Long</code> which represents the number of bytes to copy or <code>null</code> to copy until the
     *           end of the blob.
     * @param md5
     *           A <code>String</code> which represents the MD5 caluclated for the range of bytes of the source.
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
     */
    @DoesServiceRequest
    private void createBlockFromURIInternal(final String blockId, final URI copySource, final Long offset, final Long length,
                                            String md5, final AccessCondition accessCondition, BlobRequestOptions options,
                                            OperationContext opContext) throws StorageException {
        ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                createBlockFromURIImpl(blockId, copySource, offset, length, md5, accessCondition, options, opContext),
                options.getRetryPolicyFactory(), opContext);
    }

    private StorageRequest<CloudBlobClient, CloudBlob, Void> createBlockFromURIImpl(final String blockId, final URI copySource, final Long offset, final Long length,
                                                                                    final String md5, final AccessCondition accessCondition,
                                                                                    final BlobRequestOptions options, final OperationContext opContext) {

        return new StorageRequest<CloudBlobClient, CloudBlob, Void>(
                options, this.getStorageUri()) {

            @Override
            public HttpURLConnection buildRequest(CloudBlobClient client, CloudBlob blob, OperationContext context)
                    throws Exception {
                return BlobRequest.putBlock(blob.getTransformedAddress(opContext).getUri(this.getCurrentLocation()),
                        copySource.toASCIIString(), offset, length,
                        options, md5, opContext, accessCondition, blockId);
            }

            @Override
            public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context)
                    throws Exception {
                StorageRequest.signBlobQueueAndFileRequest(connection, client, 0, context);
            }

            @Override
            public Void preProcessResponse(CloudBlob blob, CloudBlobClient client, OperationContext context)
                    throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_CREATED) {
                    this.setNonExceptionedRetryableFailure(true);
                    return null;
                }

                this.getResult().setRequestServiceEncrypted(BaseResponse.isServerRequestEncrypted(this.getConnection()));
                return null;
            }
        };
    }

    /**
     * Uploads a blob from a string using the platform's default encoding. If the blob already exists on the service, it
     * will be overwritten.
     * 
     * @param content
     *            A <code>String</code> which represents the content that will be uploaded to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws IOException
     */
    public void uploadText(final String content) throws StorageException, IOException {
        this.uploadText(content, null /* charsetName */, null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Uploads a blob from a string using the specified encoding. If the blob already exists on the service, it will be 
     * overwritten.
     * 
     * @param content
     *            A <code>String</code> which represents the content that will be uploaded to the blob.
     * @param charsetName
     *            A <code>String</code> which represents the name of the charset to use to encode the content.
     *            If null, the platform's default encoding is used.
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
    public void uploadText(final String content, final String charsetName, final AccessCondition accessCondition,
            BlobRequestOptions options, OperationContext opContext) throws StorageException, IOException {
        byte[] bytes = (charsetName == null) ? content.getBytes() : content.getBytes(charsetName);
        this.uploadFromByteArray(bytes, 0, bytes.length, accessCondition, options, opContext);
    }

    /**
     * Downloads a blob to a string using the platform's default encoding.
     * 
     * @return A <code>String</code> which represents the blob's contents.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws IOException
     */
    public String downloadText() throws StorageException, IOException {
        return this
                .downloadText(null /* charsetName */, null /* accessCondition */, null /* options */, null /* opContext */);
    }

    /**
     * Downloads a blob to a string using the specified encoding.
     * 
     * @param charsetName
     *            A <code>String</code> which represents the name of the charset to use to encode the content.
     *            If null, the platform's default encoding is used.
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
     * @return A <code>String</code> which represents the blob's contents.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws IOException
     */
    public String downloadText(final String charsetName, final AccessCondition accessCondition,
            BlobRequestOptions options, OperationContext opContext) throws StorageException, IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        this.download(baos, accessCondition, options, opContext);
        return charsetName == null ? baos.toString() : baos.toString(charsetName);
    }

    /**
     * Sets the number of bytes to buffer when writing to a {@link BlobOutputStream}.
     *
     * @param streamWriteSizeInBytes An <code>int</code> which represents the maximum block size, in bytes, for writing to a block blob
     *                               while using a {@link BlobOutputStream} object, ranging from 16 KB to 100 MB, inclusive.
     * @throws IllegalArgumentException If <code>streamWriteSizeInBytes</code> is less than 16 KB or greater than 100 MB.
     */
    @Override
    public void setStreamWriteSizeInBytes(final int streamWriteSizeInBytes)
    {
        if (streamWriteSizeInBytes > Constants.MAX_BLOCK_SIZE || streamWriteSizeInBytes < Constants.MIN_PERMITTED_BLOCK_SIZE)
        {
            throw new IllegalArgumentException("StreamWriteSizeInBytes");
        }

        this.streamWriteSizeInBytes = streamWriteSizeInBytes;
        this.isStreamWriteSizeModified = true;
    }

    /**
     * Sets the blob tier on a block blob on a standard storage account.
     * @param standardBlobTier
     *            A {@link StandardBlobTier} object which represents the tier of the blob.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void uploadStandardBlobTier(final StandardBlobTier standardBlobTier) throws StorageException {
        this.uploadStandardBlobTier(standardBlobTier, null /* options */, null /* opContext */);
    }

    /**
     * Sets the tier on a block blob on a standard storage account.
     * @param standardBlobTier
     *            A {@link StandardBlobTier} object which represents the tier of the blob.
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
    public void uploadStandardBlobTier(final StandardBlobTier standardBlobTier, BlobRequestOptions options,
                                   OperationContext opContext) throws StorageException {
        assertNoWriteOperationForSnapshot();
        Utility.assertNotNull("standardBlobTier", standardBlobTier);

        if (opContext == null) {
            opContext = new OperationContext();
        }

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.BLOCK_BLOB, this.blobServiceClient);

        ExecutionEngine.executeWithRetry(this.blobServiceClient, this,
                this.uploadBlobTierImpl(standardBlobTier.toString(), options), options.getRetryPolicyFactory(), opContext);
    }

    /**
     * Gets the flag that indicates whether the default streamWriteSize was modified.
     */
    public boolean isStreamWriteSizeModified()
    {
        return this.isStreamWriteSizeModified;
    }

    private void waitAny(ExecutorCompletionService<Void> completionService) throws StorageException, IOException {
        try {
            completionService.take().get();
        }
        catch (Exception e) {
            Throwable cause = e.getCause();
            while (cause != null) {
                if (cause instanceof StorageException) {
                    throw (StorageException) cause;
                }

                cause = cause.getCause();
            }

            throw Utility.initIOException(e);
        }
    }
}