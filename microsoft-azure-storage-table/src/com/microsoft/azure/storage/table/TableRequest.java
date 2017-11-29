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

package com.microsoft.azure.storage.table;

import java.io.IOException;
import java.net.*;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.core.*;

/**
 * Reserved for internal use. A class used to generate requests for Table objects.
 */
final class TableRequest {
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
                    TableConstants.USER_AGENT_VERSION, userAgentComment);
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

    public static final void signRequest(HttpURLConnection request, ServiceClient client, long contentLength,
                                         OperationContext context) throws InvalidKeyException, StorageException {
        StorageCredentialsHelperForTable.signRequest(client.getCredentials(), request, contentLength, context);
    }

    /**
     * Reserved for internal use. Adds continuation token values to the specified query builder, if set.
     * 
     * @param builder
     *            The {@link UriQueryBuilder} object to apply the continuation token properties to.
     * @param continuationToken
     *            The {@link ResultContinuation} object containing the continuation token values to apply to the query
     *            builder. Specify <code>null</code> if no continuation token values are set.
     * 
     * @throws StorageException
     *             if an error occurs in accessing the query builder or continuation token.
     */
    private static void applyContinuationToQueryBuilder(final UriQueryBuilder builder,
            final ResultContinuation continuationToken) throws StorageException {
        if (continuationToken != null) {
            if (continuationToken.getNextPartitionKey() != null) {
                builder.add(TableConstants.TABLE_SERVICE_NEXT_PARTITION_KEY, continuationToken.getNextPartitionKey());
            }

            if (continuationToken.getNextRowKey() != null) {
                builder.add(TableConstants.TABLE_SERVICE_NEXT_ROW_KEY, continuationToken.getNextRowKey());
            }

            if (continuationToken.getNextTableName() != null) {
                builder.add(TableConstants.TABLE_SERVICE_NEXT_TABLE_NAME, continuationToken.getNextTableName());
            }
        }
    }

    /**
     * Reserved for internal use. Constructs an <code>HttpURLConnection</code> to perform a table batch operation.
     * 
     * @param rootUri
     *            A <code>java.net.URI</code> containing an absolute URI to the resource.
     * @param tableOptions
     *            A {@link TableRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. This parameter is unused.
     * @param queryBuilder
     *            The {@link UriQueryBuilder} for the operation.
     * @param opContext
     *            An {@link OperationContext} object for tracking the current operation. Specify <code>null</code> to
     *            safely ignore operation context.
     * @param batchID
     *            The <code>String</code> containing the batch identifier.
     * @return
     *         An <code>HttpURLConnection</code> to use to perform the operation.
     * 
     * @throws IOException
     *             if there is an error opening the connection.
     * @throws URISyntaxException
     *             if the resource URI is invalid.
     * @throws StorageException
     *             if a storage service error occurred during the operation.
     */
    public static HttpURLConnection batch(final URI rootUri, final TableRequestOptions tableOptions,
            final UriQueryBuilder queryBuilder, final OperationContext opContext, final String batchID)
            throws IOException, URISyntaxException, StorageException {
        final URI queryUri = PathUtility.appendPathToSingleUri(rootUri, "$batch");

        final HttpURLConnection retConnection = TableRequest.createURLConnection(queryUri, tableOptions, queryBuilder,
                opContext);

        setAcceptHeaderForHttpWebRequest(retConnection, tableOptions.getTablePayloadFormat());
        retConnection.setRequestProperty(Constants.HeaderConstants.CONTENT_TYPE,
                String.format(TableConstants.HeaderConstants.MULTIPART_MIXED_FORMAT, batchID));

        retConnection.setRequestProperty(TableConstants.HeaderConstants.MAX_DATA_SERVICE_VERSION,
                TableConstants.HeaderConstants.MAX_DATA_SERVICE_VERSION_VALUE);

        retConnection.setRequestMethod("POST");
        retConnection.setDoOutput(true);
        return retConnection;
    }

    /**
     * Reserved for internal use. Constructs the core <code>HttpURLConnection</code> to perform an operation.
     * 
     * @param rootUri
     *            A <code>java.net.URI</code> containing an absolute URI to the resource.
     * @param queryBuilder
     *            The <code>UriQueryBuilder</code> for the request.
     * @param opContext
     *            An {@link OperationContext} object for tracking the current operation.
     * @param tableName
     *            The name of the table.
     * @param identity
     *            The identity of the entity, to pass in the Service Managment REST operation URI as
     *            <code><em>tableName</em>(<em>identity</em>)</code>. If <code>null</code>, only the <em>tableName</em>
     *            value will be passed.
     * @param requestMethod
     *            The HTTP request method to set.
     * @param options
     *            A {@link TableRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. This parameter is unused.
     * @return
     *         An <code>HttpURLConnection</code> to use to perform the operation.
     * 
     * @throws IOException
     *             if there is an error opening the connection.
     * @throws URISyntaxException
     *             if the resource URI is invalid.
     * @throws StorageException
     *             if a storage service error occurred during the operation.
     */
    private static HttpURLConnection coreCreate(final URI rootUri, final TableRequestOptions tableOptions,
            final UriQueryBuilder queryBuilder, final OperationContext opContext, final String tableName,
            final String eTag, final String identity, final String requestMethod) throws IOException,
            URISyntaxException, StorageException {

        URI queryUri = null;

        // Do point query / delete etc.
        if (!Utility.isNullOrEmpty(identity)) {
            queryUri = PathUtility.appendPathToSingleUri(rootUri, tableName.concat(String.format("(%s)", identity)));
        }
        else {
            queryUri = PathUtility.appendPathToSingleUri(rootUri, tableName);
        }

        final HttpURLConnection retConnection = TableRequest.createURLConnection(queryUri, tableOptions, queryBuilder,
                opContext);

        setAcceptHeaderForHttpWebRequest(retConnection, tableOptions.getTablePayloadFormat());
        retConnection.setRequestProperty(Constants.HeaderConstants.CONTENT_TYPE,
                TableConstants.HeaderConstants.JSON_CONTENT_TYPE);

        retConnection.setRequestProperty(TableConstants.HeaderConstants.MAX_DATA_SERVICE_VERSION,
                TableConstants.HeaderConstants.MAX_DATA_SERVICE_VERSION_VALUE);

        if (!Utility.isNullOrEmpty(eTag)) {
            retConnection.setRequestProperty(Constants.HeaderConstants.IF_MATCH, eTag);
        }

        retConnection.setRequestMethod(requestMethod);
        return retConnection;
    }

    /**
     * Reserved for internal use. Constructs an <code>HttpURLConnection</code> to perform a delete operation.
     * 
     * @param rootUri
     *            A <code>java.net.URI</code> containing an absolute URI to the resource.
     * @param tableOptions
     *            A {@link TableRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudTableClient}.
     * @param queryBuilder
     *            The {@link UriQueryBuilder} for the operation.
     * @param opContext
     *            An {@link OperationContext} object for tracking the current operation. Specify <code>null</code> to
     *            safely ignore operation context.
     * @param tableName
     *            The name of the table.
     * @param identity
     *            The identity of the entity. The resulting request will be formatted as /tableName(identity) if not
     *            null or empty.
     * @param eTag
     *            The etag of the entity.
     * @return
     *         An <code>HttpURLConnection</code> to use to perform the operation.
     * 
     * @throws IOException
     *             if there is an error opening the connection.
     * @throws URISyntaxException
     *             if the resource URI is invalid.
     * @throws StorageException
     *             if a storage service error occurred during the operation.
     */
    public static HttpURLConnection delete(final URI rootUri, final TableRequestOptions tableOptions,
            final UriQueryBuilder queryBuilder, final OperationContext opContext, final String tableName,
            final String identity, final String eTag) throws IOException, URISyntaxException, StorageException {

        return coreCreate(rootUri, tableOptions, queryBuilder, opContext, tableName, eTag, identity, "DELETE");
    }

    /**
     * Reserved for internal use. Constructs an <code>HttpURLConnection</code> to perform an insert operation.
     * 
     * @param rootUri
     *            A <code>java.net.URI</code> containing an absolute URI to the resource.
     * @param tableOptions
     *            A {@link TableRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudTableClient}.
     * @param queryBuilder
     *            The {@link UriQueryBuilder} for the operation.
     * @param opContext
     *            An {@link OperationContext} object for tracking the current operation. Specify <code>null</code> to
     *            safely ignore operation context.
     * @param tableName
     *            The name of the table.
     * @param identity
     *            The identity of the entity. The resulting request will be formatted as /tableName(identity) if not
     *            null or empty.
     * @param eTag
     *            The etag of the entity, can be null for straight inserts.
     * @param updateType
     *            The {@link TableUpdateType} type of update to be performed. Specify <code>null</code> for straight
     *            inserts.
     * @return
     *         An <code>HttpURLConnection</code> to use to perform the operation.
     * 
     * @throws IOException
     *             if there is an error opening the connection.
     * @throws URISyntaxException
     *             if the resource URI is invalid.
     * @throws StorageException
     *             if a storage service error occurred during the operation.
     */
    public static HttpURLConnection insert(final URI rootUri, final TableRequestOptions tableOptions,
            final UriQueryBuilder queryBuilder, final OperationContext opContext, final String tableName,
            final String identity, final String eTag, final boolean echoContent, final TableUpdateType updateType)
            throws IOException, URISyntaxException, StorageException {
        HttpURLConnection retConnection = null;

        if (updateType == null) {
            retConnection = coreCreate(rootUri, tableOptions, queryBuilder, opContext, tableName, eTag,
                    null/* identity */, "POST");
            retConnection.setRequestProperty(TableConstants.HeaderConstants.PREFER,
                    echoContent ? TableConstants.HeaderConstants.RETURN_CONTENT
                            : TableConstants.HeaderConstants.RETURN_NO_CONTENT);
        }
        else if (updateType == TableUpdateType.MERGE) {
            tableOptions.assertNoEncryptionPolicyOrStrictMode();
            
            retConnection = coreCreate(rootUri, tableOptions, queryBuilder, opContext, tableName, null/* ETAG */,
                    identity, "POST");

            retConnection.setRequestProperty(TableConstants.HeaderConstants.X_HTTP_METHOD, "MERGE");
        }
        else if (updateType == TableUpdateType.REPLACE) {
            retConnection = coreCreate(rootUri, tableOptions, queryBuilder, opContext, tableName, null/* ETAG */,
                    identity, "PUT");
        }

        retConnection.setDoOutput(true);

        return retConnection;
    }

    /**
     * Reserved for internal use. Constructs an HttpURLConnection to perform a merge operation.
     * 
     * @param rootUri
     *            A <code>java.net.URI</code> containing an absolute URI to the resource.
     * @param tableOptions
     *            A {@link TableRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudTableClient}.
     * @param queryBuilder
     *            The {@link UriQueryBuilder} for the operation.
     * @param opContext
     *            An {@link OperationContext} object for tracking the current operation. Specify <code>null</code> to
     *            safely ignore operation context.
     * @param tableName
     *            The name of the table.
     * @param identity
     *            The identity of the entity. The resulting request will be formatted as /tableName(identity) if not
     *            null or empty.
     * @param eTag
     *            The etag of the entity.
     * @return
     *         An <code>HttpURLConnection</code> to use to perform the operation.
     * 
     * @throws IOException
     *             if there is an error opening the connection.
     * @throws URISyntaxException
     *             if the resource URI is invalid.
     * @throws StorageException
     *             if a storage service error occurred during the operation.
     */
    public static HttpURLConnection merge(final URI rootUri, final TableRequestOptions tableOptions,
            final UriQueryBuilder queryBuilder, final OperationContext opContext, final String tableName,
            final String identity, final String eTag) throws IOException, URISyntaxException, StorageException {
        tableOptions.assertNoEncryptionPolicyOrStrictMode();

        final HttpURLConnection retConnection = coreCreate(rootUri, tableOptions, queryBuilder, opContext, tableName,
                eTag, identity, "POST");
        retConnection.setRequestProperty(TableConstants.HeaderConstants.X_HTTP_METHOD, "MERGE");
        retConnection.setDoOutput(true);
        return retConnection;
    }

    /**
     * Reserved for internal use. Constructs an HttpURLConnection to perform a single entity query operation.
     * 
     * @param rootUri
     *            A <code>java.net.URI</code> containing an absolute URI to the resource.
     * @param tableOptions
     *            A {@link TableRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudTableClient}.
     * @param queryBuilder
     *            The {@link UriQueryBuilder} for the operation.
     * @param opContext
     *            An {@link OperationContext} object for tracking the current operation. Specify <code>null</code> to
     *            safely ignore operation context.
     * @param tableName
     *            The name of the table.
     * @param identity
     *            The identity of the entity. The resulting request will be formatted as /tableName(identity) if not
     *            null or empty.
     * @return
     *         An <code>HttpURLConnection</code> to use to perform the operation.
     * 
     * @throws IOException
     *             if there is an error opening the connection.
     * @throws URISyntaxException
     *             if the resource URI is invalid.
     * @throws StorageException
     *             if a storage service error occurred during the operation.
     */
    public static HttpURLConnection query(final URI rootUri, final TableRequestOptions tableOptions,
            UriQueryBuilder queryBuilder, final OperationContext opContext, final String tableName,
            final String identity, final ResultContinuation continuationToken) throws IOException, URISyntaxException,
            StorageException {
        if (queryBuilder == null) {
            queryBuilder = new UriQueryBuilder();
        }

        applyContinuationToQueryBuilder(queryBuilder, continuationToken);
        final HttpURLConnection retConnection = coreCreate(rootUri, tableOptions, queryBuilder, opContext, tableName,
                null, identity, "GET");

        return retConnection;
    }

    /**
     * Reserved for internal use. Constructs an HttpURLConnection to perform an update operation.
     * 
     * @param rootUri
     *            A <code>java.net.URI</code> containing an absolute URI to the resource.
     * @param tableOptions
     *            A {@link TableRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudTableClient}.
     * @param queryBuilder
     *            The {@link UriQueryBuilder} for the operation.
     * @param opContext
     *            An {@link OperationContext} object for tracking the current operation. Specify <code>null</code> to
     *            safely ignore operation context.
     * @param tableName
     *            The name of the table.
     * @param identity
     *            A <code>String</code> representing the identity of the entity. The resulting request will be formatted
     *            using <em>/tableName(identity)</em> if identity is not >code>null</code> or empty.
     * @param eTag
     *            The etag of the entity.
     * @return
     *         An <code>HttpURLConnection</code> to use to perform the operation.
     * 
     * @throws IOException
     *             if there is an error opening the connection.
     * @throws URISyntaxException
     *             if the resource URI is invalid.
     * @throws StorageException
     *             if a storage service error occurred during the operation.
     */
    public static HttpURLConnection update(final URI rootUri, final TableRequestOptions tableOptions,
            final UriQueryBuilder queryBuilder, final OperationContext opContext, final String tableName,
            final String identity, final String eTag) throws IOException, URISyntaxException, StorageException {
        final HttpURLConnection retConnection = coreCreate(rootUri, tableOptions, queryBuilder, opContext, tableName,
                eTag, identity, "PUT");

        retConnection.setDoOutput(true);
        return retConnection;
    }

    /**
     * Sets the ACL for the table. Sign with length of aclBytes.
     * 
     * @param rootUri
     *            A <code>java.net.URI</code> containing an absolute URI to the resource.
     * @param tableOptions
     *            A {@link TableRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudTableClient}.
     * @param opContext
     *            An {@link OperationContext} object for tracking the current operation. Specify <code>null</code> to
     *            safely ignore operation context.
     * 
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     * */
    public static HttpURLConnection setAcl(final URI rootUri, final TableRequestOptions options,
            final OperationContext opContext) throws IOException, URISyntaxException, StorageException {

        UriQueryBuilder queryBuilder = new UriQueryBuilder();
        queryBuilder.add(Constants.QueryConstants.COMPONENT, "acl");

        final HttpURLConnection retConnection = TableRequest.createURLConnection(rootUri, options, queryBuilder,
                opContext);
        retConnection.setRequestMethod("PUT");
        retConnection.setDoOutput(true);

        return retConnection;
    }

    /**
     * Constructs a web request to return the ACL for this table. Sign with no length specified.
     * 
     * @param rootUri
     *            A <code>java.net.URI</code> containing an absolute URI to the resource.
     * @param tableOptions
     *            A {@link TableRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudTableClient}.
     * @param opContext
     *            An {@link OperationContext} object for tracking the current operation. Specify <code>null</code> to
     *            safely ignore operation context.
     * 
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     */
    public static HttpURLConnection getAcl(final URI rootUri, final TableRequestOptions options,
            final OperationContext opContext) throws IOException, URISyntaxException, StorageException {

        UriQueryBuilder queryBuilder = new UriQueryBuilder();
        queryBuilder.add(Constants.QueryConstants.COMPONENT, "acl");

        final HttpURLConnection retConnection = TableRequest.createURLConnection(rootUri, options, queryBuilder,
                opContext);
        retConnection.setRequestMethod("GET");

        return retConnection;
    }

    private static void setAcceptHeaderForHttpWebRequest(HttpURLConnection retConnection,
            TablePayloadFormat payloadFormat) {
        if (payloadFormat == TablePayloadFormat.JsonFullMetadata) {
            retConnection.setRequestProperty(Constants.HeaderConstants.ACCEPT,
                    TableConstants.HeaderConstants.JSON_FULL_METADATA_ACCEPT_TYPE);
        }
        else if (payloadFormat == TablePayloadFormat.Json) {
            retConnection.setRequestProperty(Constants.HeaderConstants.ACCEPT,
                    TableConstants.HeaderConstants.JSON_ACCEPT_TYPE);
        }
        else if (payloadFormat == TablePayloadFormat.JsonNoMetadata) {
            retConnection.setRequestProperty(Constants.HeaderConstants.ACCEPT,
                    TableConstants.HeaderConstants.JSON_NO_METADATA_ACCEPT_TYPE);
        }
    }

    /**
     * Private Default Constructor.
     */
    private TableRequest() {
        // No op
    }
}
