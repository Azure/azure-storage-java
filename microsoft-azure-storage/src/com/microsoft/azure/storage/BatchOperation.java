package com.microsoft.azure.storage;

import com.microsoft.azure.storage.core.StorageRequest;
import com.microsoft.azure.storage.core.Utility;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public abstract class BatchOperation <C extends ServiceClient, P, R> {

    private final List<StorageRequest<C, P, R>> subOperations = new ArrayList<>();

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
    protected final void addSubOperation(StorageRequest<C, P, R> request) {
        Utility.assertInBounds("subOperationCount", this.subOperations.size(), 0, Constants.BATCH_MAX_REQUESTS - 1);
        subOperations.add(request);
    }
}
