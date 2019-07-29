package com.microsoft.azure.storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Exception for when one or more sub-requests within a batch request fail. This exception is a map of the
 * {@link StorageException}s to the parent objects of the sub-request. Extensions of {@link Throwable} cannot use
 * generics, so this class uses data several structures of type Object. Since only groups of the same request type can
 * be batched together, the batch caller will know the intended type in context, and can safely cast the result.
 */
public class BatchException extends StorageException {

    /**
     * Maps a successful response within a batch to the parent object of the request (i.e. Void maps to the CloudBlob
     * object used for a successful delete request, as the normal delete() method on that object returns Void in its
     * StorageRequest implementation).
     */
    private final Map<Object, Object> successfulResponses;

    /**
     * Maps a failed sub-request to the parent object of the request (i.e. StorageException from a delete blob request
     * maps to the CloudBlob object used for the delete).
     */
    private final Map<StorageException, Object> exceptions;


    BatchException(Map<Object, Object> successfulResponses, Map<BatchSubResponse, Object> failedResponses) {
        super("Batch exception", "One ore more requests in a batch operation failed", null);

        Map<StorageException, Object> exceptions = new HashMap<>(failedResponses.size());
        for (Map.Entry<BatchSubResponse, Object> response : failedResponses.entrySet()) {

            StringBuilder builder = new StringBuilder();
            try (Reader reader = new BufferedReader(new InputStreamReader(response.getKey().getBody(), StandardCharsets.UTF_8))) {
                int character;
                while ((character = reader.read()) != -1) {
                    builder.append((char)character);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            exceptions.put(
                    new StorageException(response.getKey().getStatusMessage(), builder.toString(),
                            response.getKey().getStatusCode(), null, null),
                    response.getValue());
        }

        this.successfulResponses = Collections.unmodifiableMap(successfulResponses);
        this.exceptions = Collections.unmodifiableMap(exceptions);
    }

    public Map<Object, Object> getSuccessfulResponses() {
        return successfulResponses;
    }

    public Map<StorageException, Object> getExceptions() {
        return exceptions;
    }
}
