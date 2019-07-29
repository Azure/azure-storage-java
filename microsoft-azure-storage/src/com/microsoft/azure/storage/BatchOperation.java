package com.microsoft.azure.storage;

import com.microsoft.azure.storage.core.BaseRequest;
import com.microsoft.azure.storage.core.StorageRequest;
import com.microsoft.azure.storage.core.Utility;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
public abstract class BatchOperation <C extends ServiceClient, P, R> implements Iterable<Map.Entry<StorageRequest<C, P, R>, P>> {

    private static String HTTP_LINE_ENDING = "\r\n";

    private final Map<StorageRequest<C, P, R>, P> subOperations = new LinkedHashMap<>(); // maintains iteration order

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
        subOperations.put(request, parent);
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
    protected StorageRequest<C, BatchOperation<C, P, R>, Map<P, R>> batchImpl(
            C client, final RequestOptions requestOptions) {

        return new StorageRequest<C, BatchOperation<C, P, R>, Map<P, R>>(requestOptions, client.getStorageUri()) {
            @Override
            public HttpURLConnection buildRequest(C client, BatchOperation<C, P, R> parentObject,
                    OperationContext context) throws Exception {

                byte[] body = BaseRequest.buildBatchBody(client, parentObject, context);
                this.setSendStream(new ByteArrayInputStream(body));
                this.setLength((long)body.length);

                return BaseRequest.batch(
                        client.getEndpoint(), requestOptions, context, null /* accessCondition */);
            }

            @Override
            public void setHeaders(HttpURLConnection connection, BatchOperation<C, P, R> parentObject, OperationContext context) {
                connection.setRequestProperty(Constants.HeaderConstants.CONTENT_TYPE, "multipart/mixed; boundary=batch_" + parentObject.getBatchId());
            }

            @Override
            public void signRequest(HttpURLConnection connection, C client, OperationContext context)
                    throws Exception {
                StorageRequest.signBlobQueueAndFileRequest(connection, client, this.getLength(), context);
            }

            @Override
            public Map<P, R> preProcessResponse(BatchOperation<C, P, R> parentObject, C client,
                    OperationContext context) throws Exception {
                if (this.getResult().getStatusCode() != HttpURLConnection.HTTP_ACCEPTED) {
                    throw new StorageException(this.getResult().getErrorCode(), this.getResult().getStatusMessage(), null);
                }

                return null;
            }

            @Override
            public Map<P, R> postProcessResponse(HttpURLConnection connection, BatchOperation<C, P, R> parentObject,
                    C client, OperationContext context, Map<P, R> storageObject) throws Exception {

                ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
                int bytesRead;
                byte[] readBuffer = new byte[Constants.KB];
                while ((bytesRead = connection.getInputStream().read(readBuffer, 0, readBuffer.length)) != -1) {
                    responseBuffer.write(readBuffer, 0, bytesRead);
                }
                responseBuffer.flush();

                final List<BatchSubResponse> parsedResponses = parseBatchBody(
                        responseBuffer.toByteArray(),
                        connection.getHeaderField(Constants.HeaderConstants.CONTENT_TYPE).split("boundary=")[1]);

                assertBatchSuccess(parsedResponses);

                final Map<P, R> successfulResponses = new HashMap<>();
                final Map<P, BatchSubResponse> failedResponses = new HashMap<>();

            }
        };
    }

    private P findParent(BatchSubResponse subResponse) {
        //iterate through requests, keeping track of index (Content-ID) and find the parent that matches
        int i = 0;
        for (Map.Entry<StorageRequest<C, P, R>, P> op : subOperations.entrySet()) {
            if (i == Integer.parseInt(subResponse.getHeaders().get(Constants.HeaderConstants.CONTENT_ID))) {
                return op.getValue();
            }
            i++;
        }

        /*
         * If parent isn't present, it's because batch returns a success code when the overall batch fails, and creates
         * a single sub-response that contains the actual failure response of the batch. This failure has already been
         * checked where we already declare a StorageException. We will not hit this state.
         */
        throw new IllegalStateException();
    }

    private void sortResponses(
            List<BatchSubResponse> responses,
            Map<P, R> successfulBucket,
            Map<P, BatchSubResponse> failedBucket) {

        Iterator<BatchSubResponse> it = responses.iterator();
        while (it.hasNext()) {
            BatchSubResponse response = it.next();

            if (response.getStatusCode() / 100 != 2) {
                failedBucket.put(findParent(response), response);
                it.remove();
            }
            else {
                successfulBucket.put(findParent(response), convertResponse(response));
            }
        }
    }

    /**
     * Throws if the batch as a whole failed.
     *
     * @param parsedResponses
     *      The parsed batch body.
     * @throws StorageException
     *      If the body contained a single response detailing that the batch failed.
     */
    private void assertBatchSuccess(List<BatchSubResponse> parsedResponses) throws StorageException {

        if (parsedResponses.size() != 1) {
            return;
        }

        BatchSubResponse subResponse = parsedResponses.get(0);

        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        try {
            int bytesRead;
            byte[] readBuffer = new byte[Constants.KB];
            while ((bytesRead = subResponse.getBody().read(readBuffer, 0, readBuffer.length)) != -1) {
                responseBuffer.write(readBuffer, 0, bytesRead);
            }
            responseBuffer.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }


        throw new StorageException(subResponse.getStatusMessage(), responseBuffer.toString(), subResponse.getStatusCode(), null, null);
    }

    ///// Response parsing /////


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
    private List<BatchSubResponse> parseBatchBody(byte[] body, String responseDelimiter) {
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
        String parsedStatusMessage = "";
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
                String[] lineTokens = line.split(" ");
                parsedStatusCode = Integer.parseInt(lineTokens[1]);

                if (lineTokens.length > 2) {
                    StringBuilder builder = new StringBuilder();
                    for (int i = 2; i < lineTokens.length; i++) {
                        builder.append(lineTokens[i]).append(' ');
                    }
                    builder.deleteCharAt(builder.length() - 1);
                    parsedStatusMessage = builder.toString();
                }
            }
            else { // a header line; anything else would be against protocol
                String[] tokens = line.split(":");
                parsedHeaders.put(tokens[0], tokens[1].trim());
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
        parsedResponse.setStatusMessage(parsedStatusMessage);
        parsedResponse.setHeaders(parsedHeaders);
        parsedResponse.setBody(body);

        return parsedResponse;
    }

    @Override
    public Iterator<Map.Entry<StorageRequest<C, P, R>, P>> iterator() {
        return new Iterator<Map.Entry<StorageRequest<C, P, R>, P>>() {

            final Iterator<Map.Entry<StorageRequest<C, P, R>, P>> baseIt = subOperations.entrySet().iterator();

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                return baseIt.hasNext();
            }

            @Override
            public Map.Entry<StorageRequest<C, P, R>, P> next() {
                return baseIt.next();
            }
        };
    }
}
