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
package com.microsoft.azure.storage.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.core.*;

import javax.xml.stream.XMLStreamException;

/**
 * Provides a client for accessing the Microsoft Azure File service.
 * <p>
 * This class provides a point of access to the File service. The service client encapsulates the base URI for the File
 * service. It also encapsulates the credentials for accessing the storage account.
 */
public final class CloudFileClient extends ServiceClient {

    /**
     * Holds the default request option values associated with this Service Client.
     */
    private FileRequestOptions defaultRequestOptions = new FileRequestOptions();

    /**
     * Creates an instance of the <code>CloudFileClient</code> class using the specified File service endpoint and
     * account credentials.
     * 
     * @param baseUri
     *            A <code>java.net.URI</code> object that represents the File service endpoint used to create the
     *            client.
     * @param credentials
     *            A {@link StorageCredentials} object that represents the account credentials.
     */
    public CloudFileClient(final URI baseUri, StorageCredentials credentials) {
        this(new StorageUri(baseUri), credentials);
    }

    /**
     * Creates an instance of the <code>CloudFileClient</code> class using the specified File service endpoint and
     * account credentials.
     * 
     * @param storageUri
     *            A {@link StorageUri} object that represents the File service endpoint used to create the
     *            client.
     * @param credentials
     *            A {@link StorageCredentials} object that represents the account credentials.
     */
    public CloudFileClient(StorageUri storageUri, StorageCredentials credentials) {
        super(storageUri, credentials);
        if (credentials == null || credentials.getClass().equals(StorageCredentialsAnonymous.class)) {
            throw new IllegalArgumentException(SR.STORAGE_CREDENTIALS_NULL_OR_ANONYMOUS);
        }
        FileRequestOptions.applyDefaults(this.defaultRequestOptions);
    }

    protected StorageRequest<ServiceClient, Void, ServiceProperties> downloadServicePropertiesImpl(final RequestOptions options) {
        final StorageRequest<ServiceClient, Void, ServiceProperties> getRequest = new StorageRequest<ServiceClient, Void, ServiceProperties>(
                options, this.getStorageUri()) {

            @Override
            public HttpURLConnection buildRequest(ServiceClient client, Void parentObject, OperationContext context)
                    throws Exception {
                return FileRequest.getServiceProperties(
                        credentials.transformUri(client.getEndpoint()), options, null, context);
            }

            @Override
            public void signRequest(HttpURLConnection connection, ServiceClient client, OperationContext context)
                    throws Exception {
                FileRequest.signRequest(connection, client, -1, context);
            }

            @Override
            public ServiceProperties preProcessResponse(Void parentObject, ServiceClient client,
                                                        OperationContext context) throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                    this.setNonExceptionedRetryableFailure(true);
                }

                return null;
            }

            @Override
            public ServiceProperties postProcessResponse(HttpURLConnection connection, Void parentObject,
                                                         ServiceClient client, OperationContext context, ServiceProperties storageObject) throws Exception {
                return ServicePropertiesHandler.readServicePropertiesFromStream(connection.getInputStream());
            }
        };

        return getRequest;
    }

    protected StorageRequest<ServiceClient, Void, ServiceStats> getServiceStatsImpl(final RequestOptions options) {
        final StorageRequest<ServiceClient, Void, ServiceStats> getRequest = new StorageRequest<ServiceClient, Void, ServiceStats>(
                options, this.getStorageUri()) {

            @Override
            public void setRequestLocationMode() {
                this.applyLocationModeToRequest();
                this.setRequestLocationMode(RequestLocationMode.PRIMARY_OR_SECONDARY);
            }

            @Override
            public HttpURLConnection buildRequest(ServiceClient client, Void parentObject, OperationContext context)
                    throws Exception {
                return FileRequest.getServiceStats(
                        credentials.transformUri(client.getStorageUri().getUri(this.getCurrentLocation())),
                        options, null, context);
            }

            @Override
            public void signRequest(HttpURLConnection connection, ServiceClient client, OperationContext context)
                    throws Exception {
                FileRequest.signRequest(connection, client, -1, context);
            }

            @Override
            public ServiceStats preProcessResponse(Void parentObject, ServiceClient client, OperationContext context)
                    throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                    this.setNonExceptionedRetryableFailure(true);
                }

                return null;
            }

            @Override
            public ServiceStats postProcessResponse(HttpURLConnection connection, Void parentObject,
                                                    ServiceClient client, OperationContext context, ServiceStats storageObject) throws Exception {
                return BaseRequest.defaultReadServiceStatsFromConnection(connection);
            }

        };

        return getRequest;
    }

    protected StorageRequest<ServiceClient, Void, Void> uploadServicePropertiesImpl(final ServiceProperties properties,
                                                                                    final RequestOptions options, final OperationContext opContext)
            throws StorageException {
        try {
            byte[] propertiesBytes = BaseRequest.defaultSerializeServicePropertiesToByteArray(properties);

            final ByteArrayInputStream sendStream = new ByteArrayInputStream(propertiesBytes);
            final StreamMd5AndLength descriptor = Utility.analyzeStream(sendStream, -1L, -1L,
                    true /* rewindSourceStream */, true /* calculateMD5 */);

            final StorageRequest<ServiceClient, Void, Void> putRequest = new StorageRequest<ServiceClient, Void, Void>(
                    options, this.getStorageUri()) {

                @Override
                public HttpURLConnection buildRequest(ServiceClient client, Void parentObject, OperationContext context)
                        throws Exception {
                    this.setSendStream(sendStream);
                    this.setLength(descriptor.getLength());
                    return FileRequest.setServiceProperties(
                            credentials.transformUri(client.getEndpoint()), options, null, context);
                }

                @Override
                public void setHeaders(HttpURLConnection connection, Void parentObject, OperationContext context) {
                    connection.setRequestProperty(Constants.HeaderConstants.CONTENT_MD5, descriptor.getMd5());
                }

                @Override
                public void signRequest(HttpURLConnection connection, ServiceClient client, OperationContext context)
                        throws Exception {
                    FileRequest.signRequest(connection, client, descriptor.getLength(), context);
                }

                @Override
                public Void preProcessResponse(Void parentObject, ServiceClient client, OperationContext context)
                        throws Exception {
                    if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_ACCEPTED) {
                        this.setNonExceptionedRetryableFailure(true);
                    }

                    return null;
                }

                @Override
                public void recoveryAction(OperationContext context) throws IOException {
                    sendStream.reset();
                    sendStream.mark(Constants.MAX_MARK_LENGTH);
                }
            };

            return putRequest;
        }
        catch (IllegalArgumentException e) {
            // to do : Move this to multiple catch clause so we can avoid the duplicated code once we move to Java 1.7.
            // The request was not even made. There was an error while trying to read the permissions. Just throw.
            StorageException translatedException = StorageException.translateClientException(e);
            throw translatedException;
        }
        catch (XMLStreamException e) {
            // The request was not even made. There was an error while trying to read the serviceProperties and write to stream. Just throw.
            StorageException translatedException = StorageException.translateClientException(e);
            throw translatedException;
        }
        catch (IOException e) {
            // The request was not even made. There was an error while trying to read the serviceProperties and write to stream. Just throw.
            StorageException translatedException = StorageException.translateClientException(e);
            throw translatedException;
        }
    }

    /**
     * Creates a new File service client.
     *
     * @return A {@link CloudFileClient} that represents the cloud File client.
     *
     */
    public static CloudFileClient createCloudFileClient(CloudStorageAccount cloudStorageAccount) {
        if (cloudStorageAccount.getFileStorageUri() == null) {
            throw new IllegalArgumentException(SR.FILE_ENDPOINT_NOT_CONFIGURED);
        }

        if (cloudStorageAccount.getCredentials() == null) {
            throw new IllegalArgumentException(SR.MISSING_CREDENTIALS);
        }

        if (!StorageCredentialsHelperForAccount.canCredentialsGenerateClient(cloudStorageAccount.getCredentials())) {

            throw new IllegalArgumentException(SR.CREDENTIALS_CANNOT_SIGN_REQUEST);
        }

        return new CloudFileClient(cloudStorageAccount.getFileStorageUri(), cloudStorageAccount.getCredentials());
    }

    /**
     * Gets a {@link CloudFileShare} object with the specified name.
     * 
     * @param shareName
     *            The name of the share, which must adhere to share naming rules. The share name should not
     *            include any path separator characters (/).
     *            Share names must be lowercase, between 3-63 characters long and must start with a letter or
     *            number. Share names may contain only letters, numbers, and the dash (-) character.
     * 
     * @return A reference to a {@link CloudFileShare} object.
     * @throws StorageException
     * @throws URISyntaxException
     * 
     * @see <a href="http://msdn.microsoft.com/en-us/library/azure/dn167011.aspx">Naming and Referencing Shares,
     *      Directories, Files, and Metadata</a>
     */
    public CloudFileShare getShareReference(final String shareName) throws URISyntaxException, StorageException {
        Utility.assertNotNullOrEmpty("shareName", shareName);
        return this.getShareReference(shareName, null);
    }

    /**
     * Gets a {@link CloudFileShare} object with the specified name.
     * 
     * @param shareName
     *            The name of the share, which must adhere to share naming rules. The share name should not
     *            include any path separator characters (/).
     *            Share names must be lowercase, between 3-63 characters long and must start with a letter or
     *            number. Share names may contain only letters, numbers, and the dash (-) character.
     * @param snapshotID
     *            A <code>String</code> that represents the snapshot ID of the share.
     * @return A reference to a {@link CloudFileShare} object.
     * @throws StorageException
     * @throws URISyntaxException
     * 
     * @see <a href="http://msdn.microsoft.com/en-us/library/azure/dn167011.aspx">Naming and Referencing Shares,
     *      Directories, Files, and Metadata</a>
     */
    public CloudFileShare getShareReference(final String shareName, String snapshotID) throws URISyntaxException, StorageException {
        Utility.assertNotNullOrEmpty("shareName", shareName);
        return new CloudFileShare(shareName, snapshotID, this);
    }

    /**
     * Returns an enumerable collection of shares for this File service client.
     * 
     * @return An enumerable collection of {@link CloudFileShare} objects retrieved lazily that represent the
     *         shares for this client.
     */
    @DoesServiceRequest
    public Iterable<CloudFileShare> listShares() {
        return this.listSharesWithPrefix(null, EnumSet.noneOf(ShareListingDetails.class), null /* options */, null /* opContext */);
    }

    /**
     * Returns an enumerable collection of shares whose names begin with the specified prefix for this File service
     * client.
     * 
     * @param prefix
     *            A <code>String</code> that represents the share name prefix.
     * 
     * @return An enumerable collection of {@link CloudFileShare} objects retrieved lazily that represent the
     *         shares for this client whose names begin with the specified prefix.
     */
    @DoesServiceRequest
    public Iterable<CloudFileShare> listShares(final String prefix) {
        return this.listSharesWithPrefix(prefix, EnumSet.noneOf(ShareListingDetails.class), null /* options */, null /* opContext */);
    }

    /**
     * Returns an enumerable collection of shares whose names begin with the specified prefix for this File
     * service client, using the specified details settings, request options, and operation context.
     * 
     * @param prefix
     *            A <code>String</code> that represents the share name prefix.
     * @param detailsIncluded
     *            A <code>java.util.EnumSet</code> object that contains {@link ShareListingDetails} values that indicate
     *            whether share snapshots and/or metadata will be returned.
     * @param options
     *            A {@link FileRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudFileClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return An enumerable collection of {@link CloudFileShare} objects retrieved lazily that represents the
     *         shares for this client.
     */
    @DoesServiceRequest
    public Iterable<CloudFileShare> listShares(final String prefix, final EnumSet<ShareListingDetails> detailsIncluded,
            final FileRequestOptions options, final OperationContext opContext) {
        return this.listSharesWithPrefix(prefix, detailsIncluded, options, opContext);
    }

    /**
     * Returns a result segment of an enumerable collection of shares for this File service client.
     * 
     * @return A {@link ResultSegment} object that contains a segment of the enumerable collection of
     *         {@link CloudFileShare} objects that represent the shares for this client.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public ResultSegment<CloudFileShare> listSharesSegmented() throws StorageException {
        return this.listSharesSegmented(null, EnumSet.noneOf(ShareListingDetails.class), null, null /* continuationToken */,
                null /* options */, null /* opContext */);
    }

    /**
     * Returns a result segment of an enumerable collection of shares whose names begin with the specified
     * prefix for this File service client.
     * 
     * @param prefix
     *            A <code>String</code> that represents the prefix of the share name.
     * 
     * @return A {@link ResultSegment} object that contains a segment of the enumerable collection of
     *         {@link CloudFileShare} objects that represent the shares whose names begin with the specified
     *         prefix.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public ResultSegment<CloudFileShare> listSharesSegmented(final String prefix) throws StorageException {
        return this.listSharesWithPrefixSegmented(prefix, EnumSet.noneOf(ShareListingDetails.class), null, null /* continuationToken */,
                null /* options */, null /* opContext */);
    }

    /**
     * Returns a result segment of an enumerable collection of shares whose names begin with the specified
     * prefix, using the specified listing details options, request options, and operation context.
     * 
     * @param prefix
     *            A <code>String</code> that represents the prefix of the share name.
     * @param detailsIncluded
     *            A <code>java.util.EnumSet</code> object that contains {@link ShareListingDetails} values that indicate
     *            whether share snapshots and/or metadata will be returned.
     * @param maxResults
     *            The maximum number of results to retrieve.  If <code>null</code> or greater
     *            than 5000, the server will return up to 5,000 items.  Must be at least 1.
     * @param continuationToken
     *            A {@link ResultContinuation} object that represents a continuation token returned
     *            by a previous listing operation.
     * @param options
     *            A {@link FileRequestOptions} object that specifies any additional options for
     *            the request. Specifying <code>null</code> will use the default request options
     *            from the associated service client ( {@link CloudFileClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current
     *            operation. This object is used to track requests to the storage service,
     *            and to provide additional runtime information about the operation.
     * 
     * @return A {@link ResultSegment} object that contains a segment of the enumerable collection
     *         of {@link CloudFileShare} objects that represent the shares for this client.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public ResultSegment<CloudFileShare> listSharesSegmented(final String prefix,
            final EnumSet<ShareListingDetails> detailsIncluded, final Integer maxResults,
            final ResultContinuation continuationToken, final FileRequestOptions options,
            final OperationContext opContext) throws StorageException {
        return this.listSharesWithPrefixSegmented(prefix, detailsIncluded, maxResults, continuationToken, options,
                opContext);
    }

    /**
     * Returns an enumerable collection of shares whose names begin with the specified prefix, using the
     * specified details settings, request options, and operation context.
     * 
     * @param prefix
     *            A <code>String</code> that represents the prefix of the share name.
     * @param detailsIncluded
     *            A <code>java.util.EnumSet</code> object that contains {@link ShareListingDetails} values that indicate
     *            whether share snapshots and/or metadata will be returned.
     * @param options
     *            A {@link FileRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudFileClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return An enumerable collection of {@link CloudFileShare} objects retrieved lazily that represent the
     *         shares whose names begin with the specified prefix.
     */
    private Iterable<CloudFileShare> listSharesWithPrefix(final String prefix,
            final EnumSet<ShareListingDetails> detailsIncluded, FileRequestOptions options, OperationContext opContext) {
        if (opContext == null) {
            opContext = new OperationContext();
        }

        opContext.initialize();
        options = FileRequestOptions.populateAndApplyDefaults(options, this);

        SegmentedStorageRequest segmentedRequest = new SegmentedStorageRequest();

        return new LazySegmentedIterable<CloudFileClient, Void, CloudFileShare>(this.listSharesWithPrefixSegmentedImpl(
                prefix, detailsIncluded, null, options, segmentedRequest), this, null, options.getRetryPolicyFactory(),
                opContext);
    }

    /**
     * Returns a result segment of an enumerable collection of shares whose names begin with the specified
     * prefix, using the specified listing details options, request options, and operation context.
     * 
     * @param prefix
     *            A <code>String</code> that represents the prefix of the share name.
     * @param detailsIncluded
     *            A <code>java.util.EnumSet</code> object that contains {@link ShareListingDetails} values that indicate
     *            whether share snapshots and/or metadata will be returned.
     * @param maxResults
     *            The maximum number of results to retrieve.  If <code>null</code> or greater
     *            than 5000, the server will return up to 5,000 items.  Must be at least 1.
     * @param continuationToken
     *            A {@link ResultContinuation} object that represents a continuation token returned
     *            by a previous listing operation.
     * @param options
     *            A {@link FileRequestOptions} object that specifies any additional options for
     *            the request. Specifying <code>null</code> will use the default request options
     *            from the associated service client ( {@link CloudFileClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current
     *            operation. This object is used to track requests to the storage service,
     *            and to provide additional runtime information about the operation.
     * 
     * @return A {@link ResultSegment} object that contains a segment of the enumerable collection
     *         of {@link CloudFileShare} objects that represent the shares for this client.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    private ResultSegment<CloudFileShare> listSharesWithPrefixSegmented(final String prefix,
            final EnumSet<ShareListingDetails> detailsIncluded, final Integer maxResults,
            final ResultContinuation continuationToken, FileRequestOptions options, OperationContext opContext)
            throws StorageException {
        if (opContext == null) {
            opContext = new OperationContext();
        }

        opContext.initialize();
        options = FileRequestOptions.populateAndApplyDefaults(options, this);

        Utility.assertContinuationType(continuationToken, ResultContinuationType.SHARE);

        SegmentedStorageRequest segmentedRequest = new SegmentedStorageRequest();
        segmentedRequest.setToken(continuationToken);

        return ExecutionEngine.executeWithRetry(this, null,
                this.listSharesWithPrefixSegmentedImpl(prefix, detailsIncluded, maxResults, options, segmentedRequest),
                options.getRetryPolicyFactory(), opContext);
    }

    private StorageRequest<CloudFileClient, Void, ResultSegment<CloudFileShare>> listSharesWithPrefixSegmentedImpl(
            final String prefix, final EnumSet<ShareListingDetails> detailsIncluded, final Integer maxResults,
            final FileRequestOptions options, final SegmentedStorageRequest segmentedRequest) {

        Utility.assertContinuationType(segmentedRequest.getToken(), ResultContinuationType.SHARE);

        final ListingContext listingContext = new ListingContext(prefix, maxResults);

        final StorageRequest<CloudFileClient, Void, ResultSegment<CloudFileShare>> getRequest = new StorageRequest<CloudFileClient, Void, ResultSegment<CloudFileShare>>(
                options, this.getStorageUri()) {

            @Override
            public void setRequestLocationMode() {
                this.setRequestLocationMode(Utility.getListingLocationMode(segmentedRequest.getToken()));
            }

            @Override
            public HttpURLConnection buildRequest(CloudFileClient client, Void parentObject, OperationContext context)
                    throws Exception {
                listingContext.setMarker(segmentedRequest.getToken() != null ? segmentedRequest.getToken()
                        .getNextMarker() : null);
                return FileRequest.listShares(
                        client.getCredentials().transformUri(client.getStorageUri()).getUri(this.getCurrentLocation()),
                        options, context, listingContext, detailsIncluded);
            }

            @Override
            public void signRequest(HttpURLConnection connection, CloudFileClient client, OperationContext context)
                    throws Exception {
                FileRequest.signRequest(connection, client, -1L, context);
            }

            @Override
            public ResultSegment<CloudFileShare> preProcessResponse(Void parentObject, CloudFileClient client,
                    OperationContext context) throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_OK) {
                    this.setNonExceptionedRetryableFailure(true);
                }
                return null;
            }

            @Override
            public ResultSegment<CloudFileShare> postProcessResponse(HttpURLConnection connection, Void share,
                    CloudFileClient client, OperationContext context, ResultSegment<CloudFileShare> storageObject)
                    throws Exception {
                final ListResponse<CloudFileShare> response = ShareListHandler.getShareList(this.getConnection()
                        .getInputStream(), client);
                ResultContinuation newToken = null;

                if (response.getNextMarker() != null) {
                    newToken = new ResultContinuation();
                    newToken.setNextMarker(response.getNextMarker());
                    newToken.setContinuationType(ResultContinuationType.SHARE);
                    newToken.setTargetLocation(this.getResult().getTargetLocation());
                }

                final ResultSegment<CloudFileShare> resSegment = new ResultSegment<CloudFileShare>(
                        response.getResults(), response.getMaxResults(), newToken);

                // Important for listShares because this is required by the lazy iterator between executions.
                segmentedRequest.setToken(resSegment.getContinuationToken());
                return resSegment;
            }
        };

        return getRequest;
    }

    /**
     * Retrieves the current {@link FileServiceProperties} for the given storage service. This encapsulates
     * the CORS configurations.
     * 
     * @return A {@link FileServiceProperties} object representing the current configuration of the service.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public final FileServiceProperties downloadServiceProperties() throws StorageException {
        return this.downloadServiceProperties(null /* options */, null /* opContext */);
    }

    /**
     * Retrieves the current {@link FileServiceProperties} for the given storage service. This encapsulates
     * the CORS configurations.
     * 
     * @param options
     *            A {@link FileRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client
     *            ({@link CloudFileClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return A {@link FileServiceProperties} object representing the current configuration of the service.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public final FileServiceProperties downloadServiceProperties(FileRequestOptions options, OperationContext opContext)
            throws StorageException {
        if (opContext == null) {
            opContext = new OperationContext();
        }

        opContext.initialize();
        options = FileRequestOptions.populateAndApplyDefaults(options, this);

        return new FileServiceProperties(ExecutionEngine.executeWithRetry(
                this, null, this.downloadServicePropertiesImpl(options),
                options.getRetryPolicyFactory(), opContext));
    }

    /**
     * Uploads a new {@link FileServiceProperties} configuration to the given storage service. This encapsulates
     * the CORS configurations.
     * 
     * @param properties
     *            A {@link FileServiceProperties} object which specifies the service properties to upload.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void uploadServiceProperties(final FileServiceProperties properties) throws StorageException {
        this.uploadServiceProperties(properties, null /* options */, null /* opContext */);
    }

    /**
     * Uploads a new {@link FileServiceProperties} configuration to the given storage service. This encapsulates
     * the CORS configurations.
     * 
     * @param properties
     *            A {@link FileServiceProperties} object which specifies the service properties to upload.
     * @param options
     *            A {@link FileRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client
     *            ({@link CloudFileClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    @DoesServiceRequest
    public void uploadServiceProperties(final FileServiceProperties properties, FileRequestOptions options,
            OperationContext opContext) throws StorageException {
        if (opContext == null) {
            opContext = new OperationContext();
        }

        opContext.initialize();
        options = FileRequestOptions.populateAndApplyDefaults(options, this);

        Utility.assertNotNull("properties", properties);

        ExecutionEngine.executeWithRetry(this, null,
                this.uploadServicePropertiesImpl(properties.getServiceProperties(), options, opContext),
                options.getRetryPolicyFactory(), opContext);
    }
    
    /**
     * Gets the {@link FileRequestOptions} that is used for requests associated with this <code>CloudFileClient</code>
     * 
     * @return The {@link FileRequestOptions} object containing the values used by this <code>CloudFileClient</code>
     */
    @Override
    public FileRequestOptions getDefaultRequestOptions() {
        return this.defaultRequestOptions;
    }

    /**
     * Sets the {@link FileRequestOptions} that is used for any requests associated with this
     * <code>CloudFileClient</code> object.
     * 
     * @param defaultRequestOptions
     *            A {@link FileRequestOptions} object which specifies the options to use.
     */
    public void setDefaultRequestOptions(FileRequestOptions defaultRequestOptions) {
        Utility.assertNotNull("defaultRequestOptions", defaultRequestOptions);
        this.defaultRequestOptions = defaultRequestOptions;
    }
    
    /**
     * Indicates whether path-style URIs are being used.
     * 
     * @return <code>true</code> if using path-style URIs; otherwise, <code>false</code>.
     */
    @Override
    protected boolean isUsePathStyleUris() {
        return super.isUsePathStyleUris();
    }
}
