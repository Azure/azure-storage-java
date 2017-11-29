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

package com.microsoft.azure.storage.queue;

import java.io.IOException;
import java.net.*;
import java.security.InvalidKeyException;
import java.util.HashMap;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.core.*;

/**
 * RESERVED FOR INTERNAL USE. Provides a set of methods for constructing web
 * requests for queue operations.
 */
final class QueueRequest {

    private static final String METADATA = "metadata";

    private static final String POP_RECEIPT = "popreceipt";

    private static final String PEEK_ONLY = "peekonly";

    private static final String NUMBER_OF_MESSAGES = "numofmessages";

    private static final String VISIBILITY_TIMEOUT = "visibilitytimeout";

    private static final String MESSAGE_TTL = "messagettl";

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
                    QueueConstants.USER_AGENT_VERSION, userAgentComment);
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

        builder.add(Constants.QueryConstants.COMPONENT, METADATA);
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
        StorageCredentialsHelperForQueue.signRequest(client.getCredentials(), request, contentLength, context);
    }

    /**
     * Constructs a web request to clear all the messages in the queue. Sign the
     * web request with a length of -1L.
     * 
     * @param uri
     *            A <code>URI</code> object that specifies the absolute URI to
     *            the queue.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context
     *            for the current operation. This object is used to track
     *            requests to the storage service, and to provide additional
     *            runtime information about the operation.
     * 
     * @return An <code>HttpURLConnection</code> configured for the specified
     *         operation.
     * 
     * @throws IOException
     * @throws URISyntaxException
     *             If the URI is not valid.
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static HttpURLConnection clearMessages(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext) throws URISyntaxException, IOException, StorageException {

        final HttpURLConnection request = QueueRequest.createURLConnection(uri, queueOptions, null, opContext);

        request.setRequestMethod(Constants.HTTP_DELETE);

        return request;
    }

    /**
     * Constructs a web request to create a new queue. Sign the web request with
     * a length of 0.
     * 
     * @param uri
     *            A <code>URI</code> object that specifies the absolute URI to
     *            the queue.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context
     *            for the current operation. This object is used to track
     *            requests to the storage service, and to provide additional
     *            runtime information about the operation.
     * 
     * @return An <code>HttpURLConnection</code> configured for the specified
     *         operation.
     * 
     * @throws IOException
     * @throws URISyntaxException
     *             If the URI is not valid.
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static HttpURLConnection create(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
        return QueueRequest.create(uri, queueOptions, null, opContext);
    }

    /**
     * Constructs a web request to delete the queue. Sign the web request with a
     * length of -1L.
     * 
     * @param uri
     *            A <code>URI</code> object that specifies the absolute URI to
     *            the queue.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context
     *            for the current operation. This object is used to track
     *            requests to the storage service, and to provide additional
     *            runtime information about the operation.
     * 
     * @return An <code>HttpURLConnection</code> configured for the specified
     *         operation.
     * 
     * @throws IOException
     * @throws URISyntaxException
     *             If the URI is not valid.
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static HttpURLConnection delete(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
        return delete(uri, queueOptions, null, opContext);
    }

    /**
     * Constructs a web request to delete a message from the queue. Sign the web
     * request with a length of -1L.
     * 
     * @param uri
     *            A <code>URI</code> object that specifies the absolute URI to
     *            the queue.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context
     *            for the current operation. This object is used to track
     *            requests to the storage service, and to provide additional
     *            runtime information about the operation.
     * @param popReceipt
     *            A <code>String</code> that contains the pop receipt value
     *            returned from an earlier call to {@link CloudQueueMessage#getPopReceipt} for the message to
     *            delete.
     * @return An <code>HttpURLConnection</code> configured for the specified
     *         operation.
     * 
     * @throws URISyntaxException
     *             If the URI is not valid.
     * @throws IOException
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static HttpURLConnection deleteMessage(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext, final String popReceipt) throws URISyntaxException, IOException,
            StorageException {

        final UriQueryBuilder builder = new UriQueryBuilder();
        builder.add(POP_RECEIPT, popReceipt);

        final HttpURLConnection request = QueueRequest.createURLConnection(uri, queueOptions, builder, opContext);

        request.setRequestMethod(Constants.HTTP_DELETE);

        return request;
    }

    /**
     * Constructs a web request to download user-defined metadata and the
     * approximate message count for the queue. Sign the web request with a
     * length of -1L.
     * 
     * @param uri
     *            A <code>URI</code> object that specifies the absolute URI to
     *            the queue.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context
     *            for the current operation. This object is used to track
     *            requests to the storage service, and to provide additional
     *            runtime information about the operation.
     * 
     * @return An <code>HttpURLConnection</code> configured for the specified
     *         operation.
     * 
     * @throws IOException
     * @throws URISyntaxException
     *             If the URI is not valid.
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static HttpURLConnection downloadAttributes(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
        UriQueryBuilder builder = new UriQueryBuilder();

        builder.add(Constants.QueryConstants.COMPONENT, METADATA);
        final HttpURLConnection retConnection = QueueRequest.createURLConnection(uri, queueOptions, builder, opContext);

        retConnection.setRequestMethod(Constants.HTTP_HEAD);

        return retConnection;
    }

    /**
     * Constructs a web request to return a listing of all queues in this
     * storage account. Sign the web request with a length of -1L.
     * 
     * @param uri
     *            A <code>URI</code> object that specifies the absolute URI to
     *            the storage account.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context
     *            for the current operation. This object is used to track
     *            requests to the storage service, and to provide additional
     *            runtime information about the operation.
     * @param listingContext
     *            A {@link ListingContext} object that specifies parameters for
     *            the listing operation, if any. May be <code>null</code>.
     * @param detailsIncluded
     *            A {@link QueueListingDetails} object that specifies additional
     *            details to return with the listing, if any. May be <code>null</code>.
     * @return An <code>HttpURLConnection</code> configured for the specified
     *         operation.
     * 
     * @throws IOException
     * @throws URISyntaxException
     *             If the URI is not valid.
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static HttpURLConnection list(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext, final ListingContext listingContext,
            final QueueListingDetails detailsIncluded) throws URISyntaxException, IOException, StorageException {
        final UriQueryBuilder builder = BaseRequest.getListUriQueryBuilder(listingContext);

        if (detailsIncluded == QueueListingDetails.ALL || detailsIncluded == QueueListingDetails.METADATA) {
            builder.add(Constants.QueryConstants.INCLUDE, Constants.QueryConstants.METADATA);
        }

        final HttpURLConnection request = QueueRequest.createURLConnection(uri, queueOptions, builder, opContext);

        request.setRequestMethod(Constants.HTTP_GET);

        return request;
    }

    /**
     * Constructs a web request to retrieve a specified number of messages from
     * the front of the queue without changing their visibility. Sign the web
     * request with a length of -1L.
     * 
     * @param uri
     *            A <code>URI</code> object that specifies the absolute URI to
     *            the queue.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context
     *            for the current operation. This object is used to track
     *            requests to the storage service, and to provide additional
     *            runtime information about the operation.
     * @param numberOfMessages
     *            A nonzero value that specifies the number of messages to
     *            retrieve from the queue, up to a maximum of 32.
     * @return An <code>HttpURLConnection</code> configured for the specified
     *         operation.
     * 
     * @throws IOException
     * @throws URISyntaxException
     *             If the URI is not valid.
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static HttpURLConnection peekMessages(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext, final int numberOfMessages) throws URISyntaxException, IOException,
            StorageException {

        final UriQueryBuilder builder = new UriQueryBuilder();
        builder.add(PEEK_ONLY, "true");

        if (numberOfMessages != 0) {
            builder.add(NUMBER_OF_MESSAGES, Integer.toString(numberOfMessages));
        }

        final HttpURLConnection request = QueueRequest.createURLConnection(uri, queueOptions, builder, opContext);

        request.setRequestMethod(Constants.HTTP_GET);

        return request;
    }

    /**
     * Constructs a web request to add a message to the back of the queue. Write the encoded message request body
     * generated with a call to QueueMessageSerializer#generateMessageRequestBody(String)} to the output
     * stream of the request. Sign the web request with the length of the encoded message request body.
     * 
     * @param uri
     *            A <code>URI</code> object that specifies the absolute URI to
     *            the queue.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context
     *            for the current operation. This object is used to track
     *            requests to the storage service, and to provide additional
     *            runtime information about the operation.
     * @param visibilityTimeoutInSeconds
     *            Specifies the length of time for the message to be invisible
     *            in seconds, starting when it is added to the queue. A value of
     *            0 will make the message visible immediately. The value must be
     *            greater than or equal to 0, and cannot be larger than 7 days.
     *            The visibility timeout of a message cannot be set to a value
     *            greater than the time-to-live time.
     * @param timeToLiveInSeconds
     *            Specifies the time-to-live interval for the message, in
     *            seconds. The maximum time-to-live allowed is 7 days. If this
     *            parameter is 0, the default time-to-live of 7 days is used.
     * @return An <code>HttpURLConnection</code> configured for the specified
     *         operation.
     * 
     * @throws IOException
     * @throws URISyntaxException
     *             If the URI is not valid.
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static HttpURLConnection putMessage(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext, final int visibilityTimeoutInSeconds, final int timeToLiveInSeconds)
            throws IOException, URISyntaxException, StorageException {

        final UriQueryBuilder builder = new UriQueryBuilder();

        if (visibilityTimeoutInSeconds != 0) {
            builder.add(VISIBILITY_TIMEOUT, Integer.toString(visibilityTimeoutInSeconds));
        }

        if (timeToLiveInSeconds != 0) {
            builder.add(MESSAGE_TTL, Integer.toString(timeToLiveInSeconds));
        }

        final HttpURLConnection request = QueueRequest.createURLConnection(uri, queueOptions, builder, opContext);

        request.setDoOutput(true);
        request.setRequestMethod(Constants.HTTP_POST);

        return request;
    }

    /**
     * Constructs a web request to retrieve messages from the front of the
     * queue. Sign the web request with a length of -1L.
     * 
     * @param uri
     *            A <code>URI</code> object that specifies the absolute URI to
     *            the queue.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context
     *            for the current operation. This object is used to track
     *            requests to the storage service, and to provide additional
     *            runtime information about the operation.
     * @param numberOfMessages
     *            A nonzero value that specifies the number of messages to
     *            retrieve from the queue, up to a maximum of 32.
     * @param visibilityTimeoutInSeconds
     *            Specifies the visibility timeout value in seconds, relative to
     *            server time, to make the retrieved messages invisible until
     *            the visibility timeout expires. The value must be larger than
     *            or equal to 0, and cannot be larger than 7 days. The
     *            visibility timeout of a message can be set to a value later
     *            than the expiry time, which will prevent the message from
     *            being retrieved again whether it is processed or not.
     * @return An <code>HttpURLConnection</code> configured for the specified
     *         operation.
     * 
     * @throws IOException
     * @throws URISyntaxException
     *             If the URI is not valid.
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static HttpURLConnection retrieveMessages(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext, final int numberOfMessages, final int visibilityTimeoutInSeconds)
            throws URISyntaxException, IOException, StorageException {

        final UriQueryBuilder builder = new UriQueryBuilder();

        if (numberOfMessages != 0) {
            builder.add(NUMBER_OF_MESSAGES, Integer.toString(numberOfMessages));
        }

        builder.add(VISIBILITY_TIMEOUT, Integer.toString(visibilityTimeoutInSeconds));

        final HttpURLConnection request = QueueRequest.createURLConnection(uri, queueOptions, builder, opContext);

        request.setRequestMethod("GET");

        return request;
    }

    /**
     * Constructs a web request to set user-defined metadata for the queue. Each
     * call to this operation replaces all existing metadata attached to the
     * queue. Use the {@link #addMetadata} method to specify the metadata to set
     * on the queue. To remove all metadata from the queue, call this web
     * request with no metadata added. Sign the web request with a length of 0.
     * 
     * @param uri
     *            A <code>URI</code> object that specifies the absolute URI to
     *            the queue.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context
     *            for the current operation. This object is used to track
     *            requests to the storage service, and to provide additional
     *            runtime information about the operation.
     * 
     * @return An <code>HttpURLConnection</code> configured for the specified
     *         operation.
     * 
     * @throws IOException
     * @throws URISyntaxException
     *             If the URI is not valid.
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static HttpURLConnection setMetadata(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
        return setMetadata(uri, queueOptions, null, opContext);
    }

    /**
     * Constructs a web request to update the visibility timeout of a message in
     * the queue. Optionally updates the message content if a message request
     * body is written to the output stream of the web request. The web request
     * should be signed with the length of the encoded message request body if
     * one is included, or a length of 0 if no message request body is included.
     * 
     * @param uri
     *            A <code>URI</code> object that specifies the absolute URI to
     *            the queue.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context
     *            for the current operation. This object is used to track
     *            requests to the storage service, and to provide additional
     *            runtime information about the operation.
     * @param popReceipt
     *            A <code>String</code> that contains the pop receipt value
     *            returned from an earlier call to {@link CloudQueueMessage#getPopReceipt} for the message to
     *            update.
     * @param visibilityTimeoutInSeconds
     *            Specifies the new visibility timeout value in seconds,
     *            relative to server time, to make the retrieved messages
     *            invisible until the visibility timeout expires. The value must
     *            be larger than or equal to 0, and cannot be larger than 7
     *            days. The visibility timeout of a message can be set to a
     *            value later than the expiry time, which will prevent the
     *            message from being retrieved again whether it is processed or
     *            not.
     * @return An <code>HttpURLConnection</code> configured for the specified
     *         operation.
     * 
     * @throws IOException
     * @throws URISyntaxException
     *             If the URI is not valid.
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static HttpURLConnection updateMessage(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext, final String popReceipt, final int visibilityTimeoutInSeconds)
            throws URISyntaxException, IOException, StorageException {

        final UriQueryBuilder builder = new UriQueryBuilder();

        builder.add(POP_RECEIPT, popReceipt);

        builder.add(VISIBILITY_TIMEOUT, Integer.toString(visibilityTimeoutInSeconds));

        final HttpURLConnection request = QueueRequest.createURLConnection(uri, queueOptions, builder, opContext);

        request.setDoOutput(true);
        request.setRequestMethod(Constants.HTTP_PUT);

        return request;
    }

    /**
     * Sets the ACL for the queue. Sign with length of aclBytes.
     * 
     * @param uri
     *            The absolute URI to the queue.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     * */
    public static HttpURLConnection setAcl(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
        final UriQueryBuilder builder = new UriQueryBuilder();
        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.ACL);

        final HttpURLConnection request = QueueRequest.createURLConnection(uri, queueOptions, builder, opContext);

        request.setDoOutput(true);
        request.setRequestMethod(Constants.HTTP_PUT);

        return request;
    }

    /**
     * Constructs a web request to return the ACL for this queue. Sign with no length specified.
     * 
     * @param uri
     *            The absolute URI to the container.
     * @param queueOptions
     *            A {@link QueueRequestOptions} object that specifies execution options such as retry policy and timeout
     *            settings for the operation. Specify <code>null</code> to use the request options specified on the
     *            {@link CloudQueueClient}.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * @return a HttpURLConnection configured for the operation.
     * @throws StorageException
     */
    public static HttpURLConnection getAcl(final URI uri, final QueueRequestOptions queueOptions,
            final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
        final UriQueryBuilder builder = new UriQueryBuilder();
        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.ACL);

        final HttpURLConnection request = QueueRequest.createURLConnection(uri, queueOptions, builder, opContext);

        request.setRequestMethod(Constants.HTTP_GET);

        return request;
    }

    /**
     * Private Default Ctor.
     */
    private QueueRequest() {
        // No op
    }
}
