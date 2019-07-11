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
 * generics, so the parent object of the sub-request is of type Object. Since only groups of the same request can be
 * batched together, the batch caller should know the parent type used, and can safely cast the result.
 */
public class BatchException extends RuntimeException implements Map<StorageException, Object> {

    private final Map<StorageException, Object> exceptions;

    BatchException(Map<BatchSubResponse, Object> failedResponses) {
        super("One or more requests in a batch operation failed.");

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

        this.exceptions = Collections.unmodifiableMap(exceptions);
    }


    @Override
    public int size() {
        return exceptions.size();
    }

    @Override
    public boolean isEmpty() {
        return exceptions.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return exceptions.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return exceptions.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return exceptions.get(key);
    }

    @Override
    public Object put(StorageException key, Object value) {
        return exceptions.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return exceptions.remove(key);
    }

    @Override
    public void putAll(Map<? extends StorageException, ?> m) {
        exceptions.putAll(m);
    }

    @Override
    public void clear() {
        exceptions.clear();
    }

    @Override
    public Set<StorageException> keySet() {
        return exceptions.keySet();
    }

    @Override
    public Collection<Object> values() {
        return exceptions.values();
    }

    @Override
    public Set<Entry<StorageException, Object>> entrySet() {
        return exceptions.entrySet();
    }
}
