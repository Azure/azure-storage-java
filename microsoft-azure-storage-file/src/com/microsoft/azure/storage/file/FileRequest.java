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

import java.io.IOException;
import java.net.*;
import java.security.InvalidKeyException;
import java.util.EnumSet;
import java.util.Map;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.core.*;

/**
 * RESERVED FOR INTERNAL USE. Provides a set of methods for constructing requests for file operations.
 */
final class FileRequest {

    private static final String RANGE_QUERY_ELEMENT_NAME = "range";

    private static final String RANGE_LIST_QUERY_ELEMENT_NAME = "rangelist";

    private static final String SNAPSHOTS_QUERY_ELEMENT_NAME = "snapshots";

    /**
     * Stores the user agent to send over the wire to identify the client.
     */
    private static String userAgent;

    /**
     * Gets the user agent to send over the wire to identify the client.
     *
     * @return the user agent to send over the wire to identify the client.
     */
    public static String getUserAgent() {
        if (userAgent == null) {
            String userAgentComment = String.format(Utility.LOCALE_US, "(JavaJRE %s; %s %s)",
                    System.getProperty("java.version"), System.getProperty("os.name").replaceAll(" ", ""),
                    System.getProperty("os.version"));
            userAgent = String.format("%s/%s %s", Constants.HeaderConstants.USER_AGENT_PREFIX,
                    FileConstants.USER_AGENT_VERSION, userAgentComment);
        }

        return userAgent;
    }

    /**
     * Sets the metadata. Sign with 0 length.
     *
     * @param uri
     *            The blob Uri.
     * @param timeout
     *            The timeout.
     * @param builder
     *            The builder.
     * @param opContext
     *            an object used to track the execution of the operation
     * @return a web request for performing the operation.
     * @throws StorageException
     * @throws URISyntaxException
     * @throws IOException
     * */
    public static HttpURLConnection setMetadata(final URI uri, final RequestOptions options, UriQueryBuilder builder,
                                                final OperationContext opContext) throws IOException, URISyntaxException, StorageException {

        if (builder == null) {
            builder = new UriQueryBuilder();
        }

        builder.add(Constants.QueryConstants.COMPONENT, BaseRequest.METADATA);
        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);

        retConnection.setFixedLengthStreamingMode(0);
        retConnection.setDoOutput(true);
        retConnection.setRequestMethod(Constants.HTTP_PUT);

        return retConnection;
    }

    /**
     * Creates a HttpURLConnection used to set the Analytics service properties on the storage service.
     *
     * @param uri
     *            The service endpoint.
     * @param timeout
     *            The timeout.
     * @param builder
     *            The builder.
     * @param opContext
     *            an object used to track the execution of the operation
     * @return a web request for performing the operation.
     * @throws IOException
     * @throws URISyntaxException
     * @throws StorageException
     */
    public static HttpURLConnection setServiceProperties(final URI uri, final RequestOptions options,
                                                         UriQueryBuilder builder, final OperationContext opContext) throws IOException, URISyntaxException,
            StorageException {
        if (builder == null) {
            builder = new UriQueryBuilder();
        }

        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.PROPERTIES);
        builder.add(Constants.QueryConstants.RESOURCETYPE, BaseRequest.SERVICE);

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);

        retConnection.setDoOutput(true);
        retConnection.setRequestMethod(Constants.HTTP_PUT);

        return retConnection;
    }

    /**
     * Gets the properties. Sign with no length specified.
     *
     * @param uri
     *            The Uri to query.
     * @param timeout
     *            The timeout.
     * @param builder
     *            The builder.
     * @param opContext
     *            an object used to track the execution of the operation
     * @return a web request for performing the operation.
     * @throws StorageException
     * @throws URISyntaxException
     * @throws IOException
     * */
    public static HttpURLConnection getProperties(final URI uri, final RequestOptions options, UriQueryBuilder builder,
                                                  final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
        if (builder == null) {
            builder = new UriQueryBuilder();
        }

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
        retConnection.setRequestMethod(Constants.HTTP_HEAD);

        return retConnection;
    }

    /**
     * Creates a HttpURLConnection used to retrieve the Analytics service properties from the storage service.
     *
     * @param uri
     *            The service endpoint.
     * @param timeout
     *            The timeout.
     * @param builder
     *            The builder.
     * @param opContext
     *            an object used to track the execution of the operation
     * @return a web request for performing the operation.
     * @throws IOException
     * @throws URISyntaxException
     * @throws StorageException
     */
    public static HttpURLConnection getServiceProperties(final URI uri, final RequestOptions options,
                                                         UriQueryBuilder builder, final OperationContext opContext) throws IOException, URISyntaxException,
            StorageException {
        if (builder == null) {
            builder = new UriQueryBuilder();
        }

        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.PROPERTIES);
        builder.add(Constants.QueryConstants.RESOURCETYPE, BaseRequest.SERVICE);

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
        retConnection.setRequestMethod(Constants.HTTP_GET);

        return retConnection;
    }

    /**
     * Creates a web request to get the stats of the service.
     *
     * @param uri
     *            The service endpoint.
     * @param timeout
     *            The timeout.
     * @param builder
     *            The builder.
     * @param opContext
     *            an object used to track the execution of the operation
     * @return a web request for performing the operation.
     * @throws IOException
     * @throws URISyntaxException
     * @throws StorageException
     */
    public static HttpURLConnection getServiceStats(final URI uri, final RequestOptions options,
                                                    UriQueryBuilder builder, final OperationContext opContext) throws IOException, URISyntaxException,
            StorageException {
        if (builder == null) {
            builder = new UriQueryBuilder();
        }

        builder.add(Constants.QueryConstants.COMPONENT, BaseRequest.STATS);
        builder.add(Constants.QueryConstants.RESOURCETYPE, BaseRequest.SERVICE);

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
        retConnection.setRequestMethod("GET");

        return retConnection;
    }

    /**
     * Deletes the specified resource. Sign with no length specified.
     *
     * @param uri
     *            the request Uri.
     * @param timeout
     *            the timeout for the request
     * @param builder
     *            the UriQueryBuilder for the request
     * @param opContext
     *            an object used to track the execution of the operation
     * @return a HttpURLConnection to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if there is an improperly formated URI
     * @throws StorageException
     */
    public static HttpURLConnection delete(final URI uri, final RequestOptions options, UriQueryBuilder builder,
                                           final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
        if (builder == null) {
            builder = new UriQueryBuilder();
        }

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
        retConnection.setRequestMethod(Constants.HTTP_DELETE);

        return retConnection;
    }

    /**
     * Creates the specified resource. Note request is set to setFixedLengthStreamingMode(0); Sign with 0 length.
     *
     * @param uri
     *            the request Uri.
     * @param options
     *            A {@link RequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation.
     * @param builder
     *            the UriQueryBuilder for the request
     * @param opContext
     *            an object used to track the execution of the operation
     *
     * @return a HttpURLConnection to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if there is an improperly formated URI
     * @throws StorageException
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection create(final URI uri, final RequestOptions options, UriQueryBuilder builder,
                                           final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
        if (builder == null) {
            builder = new UriQueryBuilder();
        }

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
        retConnection.setFixedLengthStreamingMode(0);
        retConnection.setDoOutput(true);
        retConnection.setRequestMethod(Constants.HTTP_PUT);

        return retConnection;
    }

    /**
     * Creates the web request.
     *
     * @param uri
     *            the request Uri.
     * @param options
     *            A {@link RequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. This parameter is unused.
     * @param builder
     *            the UriQueryBuilder for the request
     * @param opContext
     *            an object used to track the execution of the operation
     * @return a HttpURLConnection to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if there is an improperly formated URI
     * @throws StorageException
     */
    public static HttpURLConnection createURLConnection(final URI uri, final RequestOptions options,
                                                        UriQueryBuilder builder, final OperationContext opContext) throws IOException, URISyntaxException,
            StorageException {
        if (builder == null) {
            builder = new UriQueryBuilder();
        }

        if (options.getTimeoutIntervalInMs() != null && options.getTimeoutIntervalInMs() != 0) {
            builder.add(BaseRequest.TIMEOUT, String.valueOf(options.getTimeoutIntervalInMs() / 1000));
        }

        final URL resourceUrl = builder.addToURI(uri).toURL();

        // Get the proxy settings
        Proxy proxy = OperationContext.getDefaultProxy();
        if (opContext != null && opContext.getProxy() != null) {
            proxy = opContext.getProxy();
        }

        // Set up connection, optionally with proxy settings
        final HttpURLConnection retConnection;
        if (proxy != null) {
            retConnection = (HttpURLConnection) resourceUrl.openConnection(proxy);
        }
        else {
            retConnection = (HttpURLConnection) resourceUrl.openConnection();
        }

        /*
         * ReadTimeout must be explicitly set to avoid a bug in JDK 6. In certain cases, this bug causes an immediate
         * read timeout exception to be thrown even if ReadTimeout is not set.
         *
         * Both connect and read timeout are set to the same value as we have no way of knowing how to partition
         * the remaining time between these operations. The user can override these timeouts using the SendingRequest
         * event handler if more control is desired.
         */
        int timeout = Utility.getRemainingTimeout(options.getOperationExpiryTimeInMs(), options.getTimeoutIntervalInMs());
        retConnection.setReadTimeout(timeout);
        retConnection.setConnectTimeout(timeout);

        // Note : accept behavior, java by default sends Accept behavior as text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2
        retConnection.setRequestProperty(Constants.HeaderConstants.ACCEPT, Constants.HeaderConstants.XML_TYPE);
        retConnection.setRequestProperty(Constants.HeaderConstants.ACCEPT_CHARSET, Constants.UTF8_CHARSET);

        // Note : Content-Type behavior, java by default sends Content-type behavior as application/x-www-form-urlencoded for posts.
        retConnection.setRequestProperty(Constants.HeaderConstants.CONTENT_TYPE, Constants.EMPTY_STRING);

        retConnection.setRequestProperty(Constants.HeaderConstants.STORAGE_VERSION_HEADER,
                Constants.HeaderConstants.TARGET_STORAGE_VERSION);
        retConnection.setRequestProperty(Constants.HeaderConstants.USER_AGENT, getUserAgent());
        retConnection.setRequestProperty(Constants.HeaderConstants.CLIENT_REQUEST_ID_HEADER,
                opContext.getClientRequestID());

        return retConnection;
    }

    public static final void signRequest(HttpURLConnection request, ServiceClient client,
                                         long contentLength, OperationContext context) throws InvalidKeyException, StorageException {
        StorageCredentialsHelperForFile.signRequest(client.getCredentials(), request, contentLength, context);
    }

    /**
     * Generates a web request to abort a copy operation.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            The access condition to apply to the request. Only lease conditions are supported for this operation.
     * @param copyId
     *            A <code>String</code> object that identifying the copy operation.
     * @return a <code>HttpURLConnection</code> configured for the operation.
     * @throws StorageException
     *             An exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     * @throws IOException
     * @throws URISyntaxException
     */
    public static HttpURLConnection abortCopy(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition, final String copyId)
            throws StorageException, IOException, URISyntaxException {

        final UriQueryBuilder builder = new UriQueryBuilder();

        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.COPY);
        builder.add(Constants.QueryConstants.COPY_ID, copyId);

        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, builder, opContext);

        request.setFixedLengthStreamingMode(0);
        request.setDoOutput(true);
        request.setRequestMethod(Constants.HTTP_PUT);

        request.setRequestProperty(Constants.HeaderConstants.COPY_ACTION_HEADER,
                Constants.HeaderConstants.COPY_ACTION_ABORT);

        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        return request;
    }

    /**
     * Adds the properties.
     * 
     * @param request
     *            The request
     * @param properties
     *            The file properties
     */
    private static void addProperties(final HttpURLConnection request, FileProperties properties) {
        BaseRequest.addOptionalHeader(request, FileConstants.CACHE_CONTROL_HEADER, properties.getCacheControl());
        BaseRequest.addOptionalHeader(request, FileConstants.CONTENT_DISPOSITION_HEADER,
                properties.getContentDisposition());
        BaseRequest.addOptionalHeader(request, FileConstants.CONTENT_ENCODING_HEADER, properties.getContentEncoding());
        BaseRequest.addOptionalHeader(request, FileConstants.CONTENT_LANGUAGE_HEADER, properties.getContentLanguage());
        BaseRequest.addOptionalHeader(request, FileConstants.FILE_CONTENT_MD5_HEADER, properties.getContentMD5());
        BaseRequest.addOptionalHeader(request, FileConstants.CONTENT_TYPE_HEADER, properties.getContentType());
    }

    /**
     * Adds the share snapshot if present.
     * Only for listing files and directories which requires a different query param.
     * 
     * @param builder
     *            a query builder.
     * @param snapshotVersion
     *            the share snapshot version to the query builder.
     * @throws StorageException
     */
    public static void addShareSnapshot(final UriQueryBuilder builder, final String snapshotVersion)
            throws StorageException {
        if (snapshotVersion != null) {
            builder.add(Constants.QueryConstants.SHARE_SNAPSHOT, snapshotVersion);
        }
    }

    /**
     * Creates a request to copy a file, Sign with 0 length.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param source
     *            The canonical path to the source file,
     *            in the form /<account-name>/<share-name>/<directory-path>/<file-name>.
     * @param sourceAccessConditionType
     *            A type of condition to check on the source file.
     * @param sourceAccessConditionValue
     *            The value of the condition to check on the source file
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     * @throws IOException
     * @throws URISyntaxException
     */
    public static HttpURLConnection copyFrom(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition sourceAccessCondition,
            final AccessCondition destinationAccessCondition, String source)
            throws StorageException, IOException, URISyntaxException {

        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, null, opContext);

        request.setFixedLengthStreamingMode(0);
        request.setDoOutput(true);
        request.setRequestMethod(Constants.HTTP_PUT);

        request.setRequestProperty(Constants.HeaderConstants.COPY_SOURCE_HEADER, source);

        if (sourceAccessCondition != null) {
            sourceAccessCondition.applyConditionToRequest(request);
        }

        if (destinationAccessCondition != null) {
            destinationAccessCondition.applyConditionToRequest(request);
        }

        return request;
    }    

    /**
     * Adds the properties.
     * 
     * @param request
     *            The request
     * @param properties
     *            The share properties
     */
    private static void addProperties(final HttpURLConnection request, FileShareProperties properties) {
        final Integer shareQuota = properties.getShareQuota();
        BaseRequest.addOptionalHeader(
                request, FileConstants.SHARE_QUOTA_HEADER, shareQuota == null ? null : shareQuota.toString());
    }

    /**
     * Constructs a web request to create a new share. Sign with 0 length.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param properties
     *            The properties to set for the share.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection createShare(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final FileShareProperties properties)
            throws IOException, URISyntaxException, StorageException {
        final UriQueryBuilder shareBuilder = getShareUriQueryBuilder();
        final HttpURLConnection request = FileRequest.create(uri, fileOptions, shareBuilder, opContext);
        addProperties(request, properties);
        return request;
    }

    /**
     * Constructs a HttpURLConnection to delete the file, Sign with no length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the file.
     * @return a HttpURLConnection to use to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection deleteFile(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition) throws IOException,
            URISyntaxException, StorageException {
        final UriQueryBuilder builder = new UriQueryBuilder();
        final HttpURLConnection request = delete(uri, fileOptions, builder, opContext);
        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        return request;
    }

    /**
     * Constructs a web request to delete the share and all of the directories and files within it. Sign with no length
     * specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the share.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection deleteShare(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition, String snapshotVersion, DeleteShareSnapshotsOption deleteSnapshotsOption) 
                    throws IOException, URISyntaxException, StorageException {
        final UriQueryBuilder shareBuilder = getShareUriQueryBuilder();
        FileRequest.addShareSnapshot(shareBuilder, snapshotVersion);
        HttpURLConnection request = delete(uri, fileOptions, shareBuilder, opContext);
        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        switch (deleteSnapshotsOption) {
        case NONE:
            // nop
            break;
        case INCLUDE_SNAPSHOTS:
            request.setRequestProperty(Constants.HeaderConstants.DELETE_SNAPSHOT_HEADER,
                    Constants.HeaderConstants.INCLUDE_SNAPSHOTS_VALUE);
            break;
        default:
            break;
        }

        return request;
    }

    /**
     * Constructs a web request to return the ACL for this share. Sign with no length specified.
     * 
     * @param uri
     *            The absolute URI to the share.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the share.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     */
    public static HttpURLConnection getAcl(final URI uri, final FileRequestOptions fileOptions,
            final AccessCondition accessCondition, final OperationContext opContext) throws IOException,
            URISyntaxException, StorageException {
        final UriQueryBuilder builder = getShareUriQueryBuilder();
        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.ACL);

        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, builder, opContext);

        request.setRequestMethod(Constants.HTTP_GET);

        if (accessCondition != null && !Utility.isNullOrEmpty(accessCondition.getLeaseID())) {
            accessCondition.applyLeaseConditionToRequest(request);
        }

        return request;
    }

    /**
     * Constructs a HttpURLConnection to download the file, Sign with no length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the file.
     * @param snapshotVersion
     *            The snapshot version, if the share is a snapshot.
     * @param offset
     *            The offset at which to begin returning content.
     * @param count
     *            The number of bytes to return.
     * @param requestRangeContentMD5
     *            If set to true, request an MD5 header for the specified range.
     * @return a HttpURLConnection to use to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection getFile(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition, final String snapshotVersion, final Long offset,
            final Long count, boolean requestRangeContentMD5) throws IOException, URISyntaxException, StorageException {

        if (offset != null && requestRangeContentMD5) {
            Utility.assertNotNull("count", count);
            Utility.assertInBounds("count", count, 1, Constants.MAX_RANGE_CONTENT_MD5);
        }

        final UriQueryBuilder builder = new UriQueryBuilder();
        FileRequest.addShareSnapshot(builder, snapshotVersion);
        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, builder, opContext);
        request.setRequestMethod(Constants.HTTP_GET);

        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        if (offset != null) {
            long rangeStart = offset;
            long rangeEnd;
            if (count != null) {
                rangeEnd = offset + count - 1;
                request.setRequestProperty(Constants.HeaderConstants.STORAGE_RANGE_HEADER, String.format(
                        Utility.LOCALE_US, Constants.HeaderConstants.RANGE_HEADER_FORMAT, rangeStart, rangeEnd));
            }
            else {
                request.setRequestProperty(Constants.HeaderConstants.STORAGE_RANGE_HEADER, String.format(
                        Utility.LOCALE_US, Constants.HeaderConstants.BEGIN_RANGE_HEADER_FORMAT, rangeStart));
            }
        }

        if (offset != null && requestRangeContentMD5) {
            request.setRequestProperty(Constants.HeaderConstants.RANGE_GET_CONTENT_MD5, Constants.TRUE);
        }

        return request;
    }

    /**
     * Constructs a HttpURLConnection to return the file's system properties, Sign with no length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the file.
     * @return a HttpURLConnection to use to perform the operation.
     * @param snapshotVersion
     *            the snapshot version to the query builder.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection getFileProperties(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition, final String snapshotVersion) throws StorageException,
            IOException, URISyntaxException {
        final UriQueryBuilder builder = new UriQueryBuilder();

        return getProperties(uri, fileOptions, opContext, accessCondition, builder, snapshotVersion);
    }

    /**
     * Constructs a HttpURLConnection to return a list of the file's file ranges. Sign with no length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the file.
     * @param snapshotVersion
     *            the snapshot version to the query builder.
     * @return a HttpURLConnection to use to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection getFileRanges(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition, final String snapshotVersion) throws StorageException,
            IOException, URISyntaxException {

        final UriQueryBuilder builder = new UriQueryBuilder();
        addShareSnapshot(builder, snapshotVersion);
        builder.add(Constants.QueryConstants.COMPONENT, RANGE_LIST_QUERY_ELEMENT_NAME);

        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, builder, opContext);
        request.setRequestMethod(Constants.HTTP_GET);

        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        return request;
    }

    /**
     * Constructs a web request to return the user-defined metadata for this share. Sign with no length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the share.
     * @param snapshotVersion
     *            the snapshot version to the query builder.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     * */
    public static HttpURLConnection getShareProperties(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, AccessCondition accessCondition, final String snapshotVersion) throws IOException, URISyntaxException,
            StorageException {
        final UriQueryBuilder shareBuilder = getShareUriQueryBuilder();

        return getProperties(uri, fileOptions, opContext, accessCondition, shareBuilder, snapshotVersion);
    }

    /**
     * Constructs a web request to return the stats, such as usage, for this share. Sign with no length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param options
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     */
    public static HttpURLConnection getShareStats(final URI uri, final FileRequestOptions options,
            final OperationContext opContext)
            throws IOException, URISyntaxException, StorageException {
        UriQueryBuilder shareBuilder = getShareUriQueryBuilder();
        shareBuilder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.STATS);

        final HttpURLConnection retConnection = FileRequest.createURLConnection(uri, options, shareBuilder, opContext);
        retConnection.setRequestMethod(Constants.HTTP_GET);

        return retConnection;
    }

    /**
     * Gets the share Uri query builder.
     * 
     * A <CODE>UriQueryBuilder</CODE> for the share.
     * 
     * @throws StorageException
     */
    private static UriQueryBuilder getShareUriQueryBuilder() throws StorageException {
        final UriQueryBuilder uriBuilder = new UriQueryBuilder();
        try {
            uriBuilder.add(Constants.QueryConstants.RESOURCETYPE, "share");
        }
        catch (final IllegalArgumentException e) {
            throw Utility.generateNewUnexpectedStorageException(e);
        }
        return uriBuilder;
    }

    /**
     * Gets the share Uri query builder.
     * 
     * A <CODE>UriQueryBuilder</CODE> for the share.
     * 
     * @throws StorageException
     */
    private static UriQueryBuilder getDirectoryUriQueryBuilder() throws StorageException {
        final UriQueryBuilder uriBuilder = new UriQueryBuilder();
        try {
            uriBuilder.add(Constants.QueryConstants.RESOURCETYPE, "directory");
        }
        catch (final IllegalArgumentException e) {
            throw Utility.generateNewUnexpectedStorageException(e);
        }
        return uriBuilder;
    }

    /**
     * Constructs a web request to return the user-defined metadata. Sign with no length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the share.
     * @param snapshotVersion
     *            the snapshot version to the query builder.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     * */
    private static HttpURLConnection getProperties(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, AccessCondition accessCondition, final UriQueryBuilder builder,
            String snapshotVersion)
            throws IOException, URISyntaxException, StorageException {
        addShareSnapshot(builder, snapshotVersion);
        HttpURLConnection request = getProperties(uri, fileOptions, builder, opContext);

        if (accessCondition != null) {
            accessCondition.applyLeaseConditionToRequest(request);
        }

        return request;
    }

    /**
     * Constructs a request to return a listing of all shares in this storage account. Sign with no length
     * specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param listingContext
     *            A set of parameters for the listing operation.
     * @param detailsIncluded
     *            A <code>java.util.EnumSet</code> object that contains {@link ShareListingDetails} values that indicate
     *            whether share snapshots and/or metadata will be returned.
     * @return a HttpURLConnection configured for the operation.
     * @throws IOException
     * @throws URISyntaxException
     * @throws StorageException
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection listShares(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final ListingContext listingContext,
            final EnumSet<ShareListingDetails> detailsIncluded) throws URISyntaxException, IOException, StorageException {
        final UriQueryBuilder builder = BaseRequest.getListUriQueryBuilder(listingContext);

        if (detailsIncluded != null && detailsIncluded.size() > 0) {
            final StringBuilder sb = new StringBuilder();
            boolean started = false;

            if (detailsIncluded.contains(ShareListingDetails.SNAPSHOTS)) {
                started = true;
                sb.append(SNAPSHOTS_QUERY_ELEMENT_NAME);
            }
    
            if (detailsIncluded.contains(ShareListingDetails.METADATA)) {
                if (started)
                {
                    sb.append(",");
                }

                sb.append(Constants.QueryConstants.METADATA);
            }

            builder.add(Constants.QueryConstants.INCLUDE, sb.toString());
        }

        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, builder, opContext);

        request.setRequestMethod(Constants.HTTP_GET);

        return request;
    }
    
    /**
     * Constructs a web request to set user-defined metadata for the share, Sign with 0 Length.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the share.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     * */
    public static HttpURLConnection setShareMetadata(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition) throws IOException,
            URISyntaxException, StorageException {
        final UriQueryBuilder shareBuilder = getShareUriQueryBuilder();
        return setMetadata(uri, fileOptions, opContext, accessCondition, shareBuilder);
    }

    /**
     * Constructs a web request to set user-defined metadata for the directory, Sign with 0 Length.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the directory.
     * @return a HttpURLConnection configured for the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * */
    public static HttpURLConnection setDirectoryMetadata(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition) throws IOException,
            URISyntaxException, StorageException {
        final UriQueryBuilder directoryBuilder = getDirectoryUriQueryBuilder();
        return setMetadata(uri, fileOptions, opContext, accessCondition, directoryBuilder);
    }

    /**
     * Constructs a web request to create a new directory. Sign with 0 length.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection createDirectory(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
        final UriQueryBuilder directoryBuilder = getDirectoryUriQueryBuilder();
        return FileRequest.create(uri, fileOptions, directoryBuilder, opContext);
    }

    /**
     * Constructs a web request to delete the directory and all of the directories and files within it. Sign with no
     * length
     * specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the directory.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection deleteDirectory(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition) throws IOException,
            URISyntaxException, StorageException {
        final UriQueryBuilder directoryBuilder = getDirectoryUriQueryBuilder();
        HttpURLConnection request = delete(uri, fileOptions, directoryBuilder, opContext);
        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        return request;
    }

    /**
     * Constructs a web request to return the properties for this directory. Sign with no length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the directory.
     * @param snapshotVersion
     *            the snapshot version to the query builder.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     * */
    public static HttpURLConnection getDirectoryProperties(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, AccessCondition accessCondition, String snapshotVersion) throws IOException, URISyntaxException,
            StorageException {
        final UriQueryBuilder directoryBuilder = getDirectoryUriQueryBuilder();
        return getProperties(uri, fileOptions, opContext, accessCondition, directoryBuilder, snapshotVersion);
    }

    /**
     * Constructs a request to return a listing of all files and directories in this storage account. Sign with no
     * length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param listingContext
     *            A set of parameters for the listing operation.
     * @param snapshotVersion
     *            the snapshot version to the query builder.
     * @return a HttpURLConnection configured for the operation.
     * @throws IOException
     * @throws URISyntaxException
     * @throws StorageException
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection listFilesAndDirectories(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final ListingContext listingContext, String snapshotVersion) throws URISyntaxException,
            IOException, StorageException {

        final UriQueryBuilder builder = getDirectoryUriQueryBuilder();
        addShareSnapshot(builder, snapshotVersion);
        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.LIST);

        if (listingContext != null) {
            if (!Utility.isNullOrEmpty(listingContext.getMarker())) {
                builder.add(Constants.QueryConstants.MARKER, listingContext.getMarker());
            }

            if (listingContext.getMaxResults() != null && listingContext.getMaxResults() > 0) {
                builder.add(Constants.QueryConstants.MAX_RESULTS, listingContext.getMaxResults().toString());
            }

            if (!Utility.isNullOrEmpty(listingContext.getPrefix())) {
                builder.add(Constants.QueryConstants.PREFIX, listingContext.getPrefix().toString());
            }
        }

        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, builder, opContext);

        request.setRequestMethod(Constants.HTTP_GET);

        return request;
    }

    /**
     * Constructs a HttpURLConnection to upload a file.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the file.
     * @param properties
     *            The properties to set for the file.
     * @param fileSize
     *            The size of the file.
     * @return a HttpURLConnection to use to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection putFile(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition, final FileProperties properties,
            final long fileSize) throws IOException, URISyntaxException, StorageException {
        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, null, opContext);

        request.setDoOutput(true);

        request.setRequestMethod(Constants.HTTP_PUT);

        addProperties(request, properties);

        request.setFixedLengthStreamingMode(0);
        request.setRequestProperty(Constants.HeaderConstants.CONTENT_LENGTH, "0");

        request.setRequestProperty(FileConstants.FILE_TYPE_HEADER, FileConstants.FILE);
        request.setRequestProperty(FileConstants.CONTENT_LENGTH_HEADER, String.valueOf(fileSize));

        properties.setLength(fileSize);

        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        return request;
    }

    /**
     * Constructs a HttpURLConnection to upload a file range. Sign with file size for update, or 0 for clear.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the file.
     * @param range
     *            a {link @FileRange} representing the file range
     * @param operationType
     *            a {link @FileRangeOperationType} enumeration value representing the file range operation type.
     * 
     * @return a HttpURLConnection to use to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection putRange(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition, final FileRange range,
            FileRangeOperationType operationType) throws IOException, URISyntaxException, StorageException {
        final UriQueryBuilder builder = new UriQueryBuilder();
        builder.add(Constants.QueryConstants.COMPONENT, RANGE_QUERY_ELEMENT_NAME);

        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, builder, opContext);

        request.setDoOutput(true);
        request.setRequestMethod(Constants.HTTP_PUT);

        if (operationType == FileRangeOperationType.CLEAR) {
            request.setFixedLengthStreamingMode(0);
        }

        // Range write is either update or clear; required
        request.setRequestProperty(FileConstants.FILE_RANGE_WRITE, operationType.toString());
        request.setRequestProperty(Constants.HeaderConstants.STORAGE_RANGE_HEADER, range.toString());

        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        return request;
    }

    /**
     * Constructs a HttpURLConnection to set the share's properties, signed with zero length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param options
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the file.
     * @param properties
     *            The properties to upload.
     * @return a HttpURLConnection to use to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection setShareProperties(final URI uri, final FileRequestOptions options,
            final OperationContext opContext, final AccessCondition accessCondition, final FileShareProperties properties)
            throws IOException, URISyntaxException, StorageException {
        final UriQueryBuilder builder = getShareUriQueryBuilder();
        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.PROPERTIES);

        final HttpURLConnection request = FileRequest.createURLConnection(uri, options, builder, opContext);

        request.setFixedLengthStreamingMode(0);
        request.setDoOutput(true);
        request.setRequestMethod(Constants.HTTP_PUT);

        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        if (properties != null) {
            addProperties(request, properties);
        }

        return request;
    }

    /**
     * Constructs a HttpURLConnection to set the file's size, Sign with zero length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the file.
     * @param newFileSize
     *            The new file size. Set this parameter to null to keep the existing file size.
     * @return a HttpURLConnection to use to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection resize(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition, final Long newFileSize)
            throws IOException, URISyntaxException, StorageException {
        final HttpURLConnection request = setFileProperties(uri, fileOptions, opContext, accessCondition, null);

        if (newFileSize != null) {
            request.setRequestProperty(FileConstants.CONTENT_LENGTH_HEADER, newFileSize.toString());
        }

        return request;
    }

    /**
     * Sets the ACL for the share. Sign with length of aclBytes.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the share.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     * */
    public static HttpURLConnection setAcl(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition)
            throws IOException, URISyntaxException, StorageException {
        final UriQueryBuilder builder = getShareUriQueryBuilder();
        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.ACL);

        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, builder, opContext);

        request.setRequestMethod(Constants.HTTP_PUT);
        request.setDoOutput(true);

        if (accessCondition != null && !Utility.isNullOrEmpty(accessCondition.getLeaseID())) {
            accessCondition.applyLeaseConditionToRequest(request);
        }

        return request;
    }

    /**
     * Constructs a HttpURLConnection to set metadata, Sign with 0 length.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions.
     * @return a HttpURLConnection to use to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    private static HttpURLConnection setMetadata(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition, final UriQueryBuilder builder)
            throws IOException, URISyntaxException, StorageException {
        final HttpURLConnection request = setMetadata(uri, fileOptions, builder, opContext);

        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        return request;
    }

    /**
     * Constructs a HttpURLConnection to set the file's metadata, Sign with 0 length.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the file.
     * @return a HttpURLConnection to use to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection setFileMetadata(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition) throws IOException,
            URISyntaxException, StorageException {
        return setMetadata(uri, fileOptions, opContext, accessCondition, null);
    }

    /**
     * Constructs a HttpURLConnection to create a snapshot of the share.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the share.
     * @return a HttpURLConnection to use to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection snapshotShare(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition) throws IOException,
            URISyntaxException, StorageException {
        final UriQueryBuilder builder = new UriQueryBuilder();
        builder.add(Constants.QueryConstants.RESOURCETYPE, "share");
        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.SNAPSHOT);
        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, builder, opContext);

        request.setFixedLengthStreamingMode(0);
        request.setDoOutput(true);
        request.setRequestMethod(Constants.HTTP_PUT);

        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        return request;
    }

    /**
     * Constructs a HttpURLConnection to set the file's properties, Sign with zero length specified.
     * 
     * @param uri
     *            A <code>java.net.URI</code> object that specifies the absolute URI.
     * @param fileOptions
     *            A {@link FileRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudFileClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @param accessCondition
     *            An {@link AccessCondition} object that represents the access conditions for the file.
     * @param properties
     *            The properties to upload.
     * @return a HttpURLConnection to use to perform the operation.
     * @throws IOException
     *             if there is an error opening the connection
     * @throws URISyntaxException
     *             if the resource URI is invalid
     * @throws StorageException
     *             an exception representing any error which occurred during the operation.
     * @throws IllegalArgumentException
     */
    public static HttpURLConnection setFileProperties(final URI uri, final FileRequestOptions fileOptions,
            final OperationContext opContext, final AccessCondition accessCondition, final FileProperties properties)
            throws IOException, URISyntaxException, StorageException {
        final UriQueryBuilder builder = new UriQueryBuilder();
        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.PROPERTIES);

        final HttpURLConnection request = FileRequest.createURLConnection(uri, fileOptions, builder, opContext);

        request.setFixedLengthStreamingMode(0);
        request.setDoOutput(true);
        request.setRequestMethod(Constants.HTTP_PUT);

        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
        }

        if (properties != null) {
            addProperties(request, properties);
        }

        return request;
    }

    /**
     * Private Default Ctor
     */
    private FileRequest() {
        // No op
    }
}
