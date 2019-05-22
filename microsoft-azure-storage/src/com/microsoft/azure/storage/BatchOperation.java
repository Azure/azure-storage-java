package com.microsoft.azure.storage;

import com.microsoft.azure.storage.blob.BlobBatchOperation;
import com.microsoft.azure.storage.core.StorageRequest;
import com.microsoft.azure.storage.core.Utility;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * A collection of operations to be sent as a batch request. Maintains the order of requests as added to the batch.
 *
 * @param <C>
 *     The ServiceClient type of the Storage service this batch targets.
 * @param <P>
 *     The type of the parent object making the REST call.
 * @param <R>
 */
public abstract class BatchOperation <C extends ServiceClient, P, R> implements Iterable<Map.Entry< StorageRequest<C, P, R>, P>> {

    private static String HTTP_LINE_ENDING = "\r\n";

    private final List<Map.Entry<StorageRequest<C, P, R>, P>> subOperations = new ArrayList<>();

    private final UUID batchId = UUID.randomUUID();

    /**
     * Adds an operation to the subOperations collection.
     *
     * @param request
     *          The request to add.
     *
     * @throws IllegalArgumentException
     *          Throws if this batch is already at max subOperations size. See {@link Constants#BATCH_MAX_REQUESTS}.
     */
    protected final void addSubOperation(final StorageRequest<C, P, R> request, final P parent) {
        Utility.assertInBounds("subOperationCount", this.subOperations.size(), 0, Constants.BATCH_MAX_REQUESTS - 1);
        subOperations.add(new Map.Entry<StorageRequest<C, P, R>, P>() {
            final StorageRequest<C, P, R> storageRequest = request;
            P parentObject = parent;

            @Override
            public StorageRequest<C, P, R> getKey() {
                return storageRequest;
            }

            @Override
            public P getValue() {
                return parentObject;
            }

            @Override
            public P setValue(P value) {
                parentObject = value;
                return parentObject;
            }
        });
    }

    public UUID getBatchId() {
        return batchId;
    }

    /**
     * Converts a batch sub-response from it's basic HTTP form to the response type of the operation being batched.
     *
     * @param response
     *      Object model of the HTTP response.
     *
     * @return
     *      Parsed response.
     */
    protected abstract R convertResponse(BatchSubResponse response);

    /**
     * Creates a {@link StorageRequest} for a batch operation based on this object's collected requests to make.
     *
     * @param client
     *      The {@link ServiceClient} making this request.
     * @param requestOptions
     *      Request options for this request.
     *
     * @return
     *      The built request.
     */
    protected StorageRequest<C, BlobBatchOperation<P, R>, Iterable<R>> batchImpl(
            C client, RequestOptions requestOptions) {

        return new StorageRequest<C, BlobBatchOperation<P, R>, Iterable<R>>(requestOptions, client.getStorageUri()) {
            @Override
            public HttpURLConnection buildRequest(C client, BlobBatchOperation<P, R> parentObject,
                    OperationContext context) throws Exception {
                //this.setSendStream();
                //this.setLength();

                return null;
            }

            @Override
            public void signRequest(HttpURLConnection connection, C client, OperationContext context)
                    throws Exception {
                StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, context);
            }

            @Override
            public Iterable<R> preProcessResponse(BlobBatchOperation<P, R> parentObject, C client,
                    OperationContext context) throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_ACCEPTED) {
                    throw new StorageException(this.getResult().getErrorCode(), this.getResult().getStatusMessage(), null);
                }

                return null;
            }

            @Override
            public Iterable<R> postProcessResponse(HttpURLConnection connection, BlobBatchOperation<P, R> parentObject,
                    C client, OperationContext context, Iterable<R> storageObject) throws Exception {

                ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
                int bytesRead;
                byte[] readBuffer = new byte[Constants.KB];
                while ((bytesRead = connection.getInputStream().read(readBuffer, 0, readBuffer.length)) != -1) {
                    responseBuffer.write(readBuffer, 0, bytesRead);
                }
                responseBuffer.flush();

                Iterable<BatchSubResponse> parsedResponses =
                        parseBatchBody(responseBuffer.toByteArray(), "batchResponse_" + parentObject.getBatchId());

                return null;
            }
        };
    }


    /**
     * Splits the batch body into it's sub-responses based on an HTTP mixed/multipart delimiter.
     *
     * @param body
     *      The body to split.
     * @param responseDelimiter
     *      The delimiter to split on.
     *
     * @return
     *      The sub-responses.
     */
    private Iterable<BatchSubResponse> parseBatchBody(byte[] body, String responseDelimiter) {
        List<byte[]> subResponses = Utility.splitOnPattern(
                body, (HTTP_LINE_ENDING + "--" + responseDelimiter + HTTP_LINE_ENDING).getBytes(StandardCharsets.UTF_8));

        // strip first, slightly different delim "--<delim>\r\n" off first entry
        subResponses.set(
                0,
                Utility.splitOnPattern(
                        subResponses.get(0),
                        ("--" + responseDelimiter + HTTP_LINE_ENDING).getBytes(StandardCharsets.UTF_8))
                        .get(0));

        // strip final, slightly different delim "\r\n--<delim>--" off last entry
        subResponses.set(
                subResponses.size() - 1,
                Utility.splitOnPattern(
                        subResponses.get(subResponses.size() - 1),
                        (HTTP_LINE_ENDING + "--" + responseDelimiter + "--").getBytes(StandardCharsets.UTF_8))
                        .get(0));

        List<BatchSubResponse> responses = new ArrayList<>();
        for (byte[] subResponse : subResponses) {
            responses.add(parseResponse(subResponse));
        }

        return responses;
    }

    /**
     * Parses a serialized sub-response into an object model.
     *
     * @param response
     *      The serialized sub-response.
     *
     * @return
     *      The parsed sub-response.
     */
    private BatchSubResponse parseResponse(byte[] response) {
        /*
         * Header: Value (1 or more times)
         *
         * HTTP/<version> <statusCode> <statusName>
         * Header: Value (1 or more times)
         *
         * body (if any)
         */

        int parsedStatusCode = 0;
        final Map<String, String> parsedHeaders = new HashMap<>();
        InputStream body = null;

        int lineStart = 0; // inclusive
        int lineEnd;       // exclusive
        int numBlankLinesFound = 0;
        byte[] newlinePattern = HTTP_LINE_ENDING.getBytes();

        /*
         * Java's only built in line-reading tool on a byte array is BufferedReader.readLine().
         * This forces everything to be treated as chars, leading to potential interpretation
         * issues when working with a response body that could be raw binary. To prevent this,
         * we must read raw bytes, looking for newlines, etc.. We can safely convert anything
         * not the HTTP body into strings.
         */
        while (numBlankLinesFound < 2 && lineStart < response.length) {

            lineEnd = (lineEnd = Utility.findPattern(response, newlinePattern, lineStart)) == -1
                    ? response.length
                    : lineEnd;

            String line = new String(Arrays.copyOfRange(response, lineStart, lineEnd));

            if (line.equals("")) {
                numBlankLinesFound++;
            }
            else if (line.startsWith("HTTP")) { // HTTP response
                parsedStatusCode = Integer.parseInt(line.split(" ")[1]);
            }
            else { // a header line; anything else would be against protocol
                String[] tokens = line.split(": ");
                parsedHeaders.put(tokens[0], tokens[1]);
            }

            lineStart = lineEnd + newlinePattern.length;
        }

        // response body (if any)
        if (lineStart < response.length) {
            // cannot read line by line, we need to preserve all bytes, as data can be just binary
            body = new ByteArrayInputStream(response, lineStart, response.length - lineStart);
        }

        BatchSubResponse parsedResponse = new BatchSubResponse();
        parsedResponse.setStatusCode(parsedStatusCode);
        parsedResponse.setHeaders(parsedHeaders);
        parsedResponse.setBody(body);

        return parsedResponse;
    }

    @Override
    public Iterator<Map.Entry<StorageRequest<C, P, R>, P>> iterator() {
        return this.subOperations.iterator();
    }
}
