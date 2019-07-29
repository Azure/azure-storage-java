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
 * generics, so this class uses data several structures with wildcards. Since only groups of the same request type can
 * be batched together, the batch caller will know the intended type in context, and can safely cast the result.
 */
public class BatchException extends StorageException {

    /**
     * Maps the parent object of the request to a successful response within a batch (i.e. the CloudBlob object used for
     * a successful delete request maps to Void, as the normal delete() method on that object returns Void in its
     * StorageRequest implementation).
     */
    private final Map<?, ?> successfulResponses;

    /**
     * Maps the parent object of the request to a failed sub-request (i.e. StorageException from a delete blob request
     * maps to the CloudBlob object used for the delete).
     */
    private final Map<?, StorageException> exceptions;


    BatchException(Map<?, ?> successfulResponses, Map<?, BatchSubResponse> failedResponses) {
        super("Batch exception", "One ore more requests in a batch operation failed", null);

        Map<Object, StorageException> exceptions = new HashMap<>(failedResponses.size());
        for (Map.Entry<?, BatchSubResponse> response : failedResponses.entrySet()) {

            StringBuilder builder = new StringBuilder();
            try (Reader reader = new BufferedReader(new InputStreamReader(response.getValue().getBody(), StandardCharsets.UTF_8))) {
                int character;
                while ((character = reader.read()) != -1) {
                    builder.append((char)character);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            exceptions.put(
                    response.getKey(),
                    new StorageException(response.getValue().getStatusMessage(), builder.toString(),
                            response.getValue().getStatusCode(), null, null));
        }

        this.successfulResponses = Collections.unmodifiableMap(successfulResponses);
        this.exceptions = Collections.unmodifiableMap(exceptions);
    }

    public Map<?, ?> getSuccessfulResponses() {
        return successfulResponses;
    }

    public Map<?, StorageException> getExceptions() {
        return exceptions;
    }
}
