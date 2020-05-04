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
package com.microsoft.azure.storage.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.PasswordAuthentication;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RequestOptions;
import com.microsoft.azure.storage.StorageException;

import javax.net.ssl.HttpsURLConnection;

import static com.microsoft.azure.storage.Constants.QueryConstants.PROPERTIES;

/**
 * RESERVED FOR INTERNAL USE. The Base Request class for the protocol layer.
 */
public final class BaseRequest {

    private static final String METADATA = "metadata";

    private static final String SERVICE = "service";

    private static final String STATS = "stats";

    private static final String TIMEOUT = "timeout";

    private static final String ACCOUNT = "account";

    private static final String USER_DELEGATION_KEY = "userdelegationkey";

    private static final String BATCH_QUERY_ELEMENT_NAME = "batch";

    private static final String HTTP_LINE_ENDING = "\r\n";

    /**
     * Stores the user agent to send over the wire to identify the client.
     */
    private static String userAgent;

    /**
     * Adds the metadata.
     *
     * @param request
     *            The request.
     * @param metadata
     *            The metadata.
     */
    public static void addMetadata(final HttpURLConnection request, final Map<String, String> metadata,
            final OperationContext opContext) {
        if (metadata != null) {
            for (final Entry<String, String> entry : metadata.entrySet()) {
                addMetadata(request, entry.getKey(), entry.getValue(), opContext);
            }
        }
    }

    /**
     * Adds the metadata.
     *
     * @param opContext
     *            an object used to track the execution of the operation
     * @param request
     *            The request.
     * @param name
     *            The metadata name.
     * @param value
     *            The metadata value.
     */
    private static void addMetadata(final HttpURLConnection request, final String name, final String value,
            final OperationContext opContext) {
        if (Utility.isNullOrEmptyOrWhitespace(name)) {
            throw new IllegalArgumentException(SR.METADATA_KEY_INVALID);
        }
        else if (Utility.isNullOrEmptyOrWhitespace(value)) {
            throw new IllegalArgumentException(SR.METADATA_VALUE_INVALID);
        }

        request.setRequestProperty(Constants.HeaderConstants.PREFIX_FOR_STORAGE_METADATA + name, value);
    }

    /**
     * Adds the optional header.
     *
     * @param request
     *            a HttpURLConnection for the operation.
     * @param name
     *            the metadata name.
     * @param value
     *            the metadata value.
     */
    public static void addOptionalHeader(final HttpURLConnection request, final String name, final String value) {
        if (value != null && !value.equals(Constants.EMPTY_STRING)) {
            request.setRequestProperty(name, value);
        }
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
            builder.add(TIMEOUT, String.valueOf(options.getTimeoutIntervalInMs() / 1000));
        }

        final URL resourceUrl = builder.addToURI(uri).toURL();

        // Get the proxy settings
        Proxy proxy = OperationContext.getDefaultProxy();
        String username = OperationContext.getDefaultProxyUsername();
        String password = OperationContext.getDefaultProxyPassword();
        if (opContext != null && opContext.getProxy() != null) {
            proxy = opContext.getProxy();
            if (opContext.getProxyUsername() != null) {
                username = opContext.getProxyUsername();
            }
            if (opContext.getProxyPassword() != null) {
                password = opContext.getProxyPassword();
            }
        }

        // Set up connection, optionally with proxy settings
        final HttpURLConnection retConnection;
        if (proxy != null) {
            retConnection = (HttpURLConnection) resourceUrl.openConnection(proxy);
            if (username != null && password != null) {
                String authString = "Basic " + Utility.safeEncode(username + ":" + password);
                final String authUsername = username;
                final String authPassword = password;
                retConnection.setRequestProperty("Proxy-Authorization", authString);
                Authenticator.setDefault(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        PasswordAuthentication passwordAuthentication =
                                new PasswordAuthentication (authUsername, authPassword.toCharArray());
                        return passwordAuthentication;
                    }
                });
            }
        }
        else {
            retConnection = (HttpURLConnection) resourceUrl.openConnection();
        }

        /*
        If we are using https, check if we should enable socket keep-alive timeouts to work around JVM bug.
         */
        if (retConnection instanceof HttpsURLConnection && !options.disableHttpsSocketKeepAlive()) {
            HttpsURLConnection httpsConnection = ((HttpsURLConnection) retConnection);
            httpsConnection.setSSLSocketFactory(KeepAliveSocketFactory.getInstance());
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

    /**
     * Deletes the specified resource. Sign with no length specified.
     *
     * @param uri
     *            the request Uri.
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
     * Un-deletes the specified resource. Sign with no length specified.
     *
     * @param uri
     *            the request Uri.
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
    public static HttpURLConnection undelete(final URI uri, final RequestOptions options, UriQueryBuilder builder,
                                           final OperationContext opContext) throws IOException, URISyntaxException, StorageException {
        if (builder == null) {
            builder = new UriQueryBuilder();
        }
        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.UNDELETE);

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
        retConnection.setFixedLengthStreamingMode(0);
        retConnection.setDoOutput(true);
        retConnection.setRequestMethod(Constants.HTTP_PUT);
        return retConnection;
    }

    /**
     * Gets a {@link UriQueryBuilder} for listing.
     *
     * @param listingContext
     *            A {@link ListingContext} object that specifies parameters for
     *            the listing operation, if any. May be <code>null</code>.
     *
     * @throws StorageException
     *             If a storage service error occurred during the operation.
     */
    public static UriQueryBuilder getListUriQueryBuilder(final ListingContext listingContext) throws StorageException {
        final UriQueryBuilder builder = new UriQueryBuilder();
        builder.add(Constants.QueryConstants.COMPONENT, Constants.QueryConstants.LIST);

        if (listingContext != null) {
            if (!Utility.isNullOrEmpty(listingContext.getPrefix())) {
                builder.add(Constants.QueryConstants.PREFIX, listingContext.getPrefix());
            }

            if (!Utility.isNullOrEmpty(listingContext.getMarker())) {
                builder.add(Constants.QueryConstants.MARKER, listingContext.getMarker());
            }

            if (listingContext.getMaxResults() != null && listingContext.getMaxResults() > 0) {
                builder.add(Constants.QueryConstants.MAX_RESULTS, listingContext.getMaxResults().toString());
            }
        }

        return builder;
    }

    /**
     * Gets the properties. Sign with no length specified.
     *
     * @param uri
     *            The Uri to query.
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

        builder.add(Constants.QueryConstants.COMPONENT, PROPERTIES);
        builder.add(Constants.QueryConstants.RESOURCETYPE, SERVICE);

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
        retConnection.setRequestMethod(Constants.HTTP_GET);

        return retConnection;
    }

    /**
     * Creates a web request to get the stats of the service.
     *
     * @param uri
     *            The service endpoint.
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

        builder.add(Constants.QueryConstants.COMPONENT, STATS);
        builder.add(Constants.QueryConstants.RESOURCETYPE, SERVICE);

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
        retConnection.setRequestMethod("GET");

        return retConnection;
    }

    public static HttpURLConnection getAccountInfo(final URI uri, final RequestOptions options,
            UriQueryBuilder builder, final OperationContext opContext) throws IOException, URISyntaxException,
            StorageException {
        if (builder == null) {
            builder = new UriQueryBuilder();
        }

        builder.add(Constants.QueryConstants.RESOURCETYPE, ACCOUNT);
        builder.add(Constants.QueryConstants.COMPONENT, PROPERTIES);

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);
        retConnection.setRequestMethod("HEAD");

        return retConnection;
    }

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
                    Constants.HeaderConstants.USER_AGENT_VERSION, userAgentComment);
        }

        return userAgent;
    }

    /**
     * Sets the metadata. Sign with 0 length.
     *
     * @param uri
     *            The blob Uri.
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

        builder.add(Constants.QueryConstants.COMPONENT, PROPERTIES);
        builder.add(Constants.QueryConstants.RESOURCETYPE, SERVICE);

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);

        retConnection.setDoOutput(true);
        retConnection.setRequestMethod(Constants.HTTP_PUT);

        return retConnection;
    }

    /**
     * Creates a HttpURLConnection used to request a {@link com.microsoft.azure.storage.UserDelegationKey}
     * from the service.
     * @param uri
     *            The service endpoint.
     * @param options
     *            The options for the http request.
     * @param builder
     *            The builder.
     * @param opContext
     *            An object used to track the execution of the operation.
     * @return a web request for performing the operation.
     * @throws IOException
     * @throws URISyntaxException
     * @throws StorageException
     */
    public static HttpURLConnection getUserDelegationKey(final URI uri, final RequestOptions options,
            UriQueryBuilder builder, final OperationContext opContext)
            throws IOException, URISyntaxException, StorageException {
        if (builder == null) {
            builder = new UriQueryBuilder();
        }

        builder.add(Constants.QueryConstants.COMPONENT, USER_DELEGATION_KEY);
        builder.add(Constants.QueryConstants.RESOURCETYPE, SERVICE);

        final HttpURLConnection retConnection = createURLConnection(uri, options, builder, opContext);

        retConnection.setDoOutput(true);
        retConnection.setRequestMethod(Constants.HTTP_POST);

        return retConnection;
    }

    public static HttpURLConnection batch(final URI uri, final RequestOptions options,
            final OperationContext opContext, final AccessCondition accessCondition)
            throws IOException, URISyntaxException, StorageException {

        final UriQueryBuilder builder = new UriQueryBuilder();
        builder.add(Constants.QueryConstants.COMPONENT, BATCH_QUERY_ELEMENT_NAME);

        final HttpURLConnection request = createURLConnection(uri, options, builder, opContext);

        request.setDoOutput(true);
        request.setRequestMethod(Constants.HTTP_POST);

        if (accessCondition != null) {
            accessCondition.applyConditionToRequest(request);
            accessCondition.applyAppendConditionToRequest(request);
        }

        return request;
    }

    public static <C extends ServiceClient, P, R> byte[] buildBatchBody(final C client,
            final BatchOperation<C, P, R> batch, final OperationContext opContext) throws Exception {

        // prebuilt byte arrays for reuse in writing to stream
        final byte[] delim = ("--batch_" + batch.getBatchId() + HTTP_LINE_ENDING).getBytes(StandardCharsets.UTF_8);
        final byte[] contentTypeHeader = ("Content-Type: application/http" + HTTP_LINE_ENDING).getBytes(StandardCharsets.UTF_8);
        final byte[] newlineBytes = HTTP_LINE_ENDING.getBytes(StandardCharsets.UTF_8);

        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        int contentId = 0;
        for (Map.Entry<com.microsoft.azure.storage.core.StorageRequest<C, P, R>, P> subRequest : batch) {
            // build request
            subRequest.getKey().initializeLocation();
            HttpURLConnection builtRequest = copyRequestForBatch(subRequest.getKey().buildRequest(client, subRequest.getValue(), opContext));
            subRequest.getKey().setHeaders(builtRequest, subRequest.getValue(), opContext);
            String authValue =  StorageRequest.signBlobQueueAndFileRequest(builtRequest, client, -1L, opContext);

            stream.write(delim);

            // apply specific headers
            stream.write(contentTypeHeader);
            stream.write((Constants.HeaderConstants.CONTENT_ID + ": " + contentId++ + HTTP_LINE_ENDING).getBytes(StandardCharsets.UTF_8));
            if (builtRequest.getRequestProperties().get(Constants.HeaderConstants.CONTENT_TRANSFER_ENCODING) != null) {
                stream.write((Constants.HeaderConstants.CONTENT_TRANSFER_ENCODING + ": " +
                        builtRequest.getRequestProperties().get(Constants.HeaderConstants.CONTENT_TRANSFER_ENCODING)
                        + HTTP_LINE_ENDING).getBytes(StandardCharsets.UTF_8));
            }

            stream.write(newlineBytes);

            String requestMethod = builtRequest.getRequestMethod();
            String path = builtRequest.getURL().getPath();
            String query = (query = builtRequest.getURL().getQuery()) == null
                    ? ""
                    : "?" + query;
            stream.write((requestMethod + " " + path + query + " HTTP/1.1" +  HTTP_LINE_ENDING).getBytes(StandardCharsets.UTF_8));

            for (Map.Entry<String, List<String>> header : builtRequest.getRequestProperties().entrySet()) {
                stream.write((header.getKey() + ": " + Utility.stringJoin(",", header.getValue()) + HTTP_LINE_ENDING).getBytes(StandardCharsets.UTF_8));
            }
            stream.write((Constants.HeaderConstants.AUTHORIZATION + ": " + authValue + HTTP_LINE_ENDING).getBytes(StandardCharsets.UTF_8));

            stream.write(newlineBytes);

            // TODO serialize sub-request body
        }

        stream.write(("--batch_" + batch.getBatchId() + "--" + HTTP_LINE_ENDING).getBytes(StandardCharsets.UTF_8));

        return stream.toByteArray();
    }

    /**
     * Creates a new HttpURLConnection for batch requests.
     * Java does not give the ability to remove a header from a URLConnection, and batch requires us to remove the
     * x-ms-version header, as well as specially treat some others. Also, we cannot just refuse to serialize it in our
     * manual serialization, as we must apply our signing logic, which operates on the HttpURLConnection, and so would
     * improperly sign the version header into the signature. Rather than try and prevent them from being set in the
     * first place, this method copies the HttpURLConnection, and specifically avoids copying over some headers. This
     * method also does not worry about completely duplicating every aspect of the HttpURLConnection, as we will not
     * actually use it to send out a request.
     *
     * @param connection
     * @return
     */
    private static HttpURLConnection copyRequestForBatch(HttpURLConnection connection) {
        HttpURLConnection result;
        try {
            result = (HttpURLConnection)connection.getURL().openConnection();
            result.setRequestMethod(connection.getRequestMethod());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (Map.Entry<String, List<String>> header : connection.getRequestProperties().entrySet()) {
            if (!header.getKey().equals(Constants.HeaderConstants.STORAGE_VERSION_HEADER) &&
                    !header.getKey().equals(Constants.HeaderConstants.CONTENT_TYPE) &&
                    !header.getKey().equals(Constants.HeaderConstants.CONTENT_TRANSFER_ENCODING)) {
                result.setRequestProperty(header.getKey(), Utility.stringJoin(",", header.getValue()));
            }
        }

        return result;
    }

    /**
     * A private default constructor. All methods of this class are static so no instances of it should ever be created.
     */
    private BaseRequest() {
        // No op
    }
}
