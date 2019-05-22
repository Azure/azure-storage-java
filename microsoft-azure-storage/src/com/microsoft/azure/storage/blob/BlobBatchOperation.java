package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.BatchOperation;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.core.ExecutionEngine;

public abstract class BlobBatchOperation<P, R> extends BatchOperation<CloudBlobClient, P, R> {

    final Iterable<R> execute(CloudBlobClient client, BlobRequestOptions requestOptions, OperationContext operationContext)
            throws StorageException {
        if (operationContext == null) {
            operationContext = new OperationContext();
        }

        operationContext.initialize();
        // This line is why we put this method in BlobBatchOperation and not BatchOperation.
        // The method requires a CloudBlobClient, not just a service client.
        requestOptions = BlobRequestOptions.populateAndApplyDefaults(requestOptions, BlobType.UNSPECIFIED, client);

        return ExecutionEngine.executeWithRetry(
                client,
                this,
                this.batchImpl(client, requestOptions),
                requestOptions.getRetryPolicyFactory(),
                operationContext);
    }
}
