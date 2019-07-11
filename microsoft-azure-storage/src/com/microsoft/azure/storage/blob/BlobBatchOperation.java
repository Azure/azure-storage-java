package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.core.ExecutionEngine;

import java.util.Map;

public abstract class BlobBatchOperation<P, R> extends BatchOperation<CloudBlobClient, P, R> {

    final Iterable<Map.Entry<P, R>> execute(CloudBlobClient client, BlobRequestOptions requestOptions, OperationContext operationContext)
            throws StorageException {

        if (operationContext == null) {
            operationContext = new OperationContext();
        }
        operationContext.initialize();

        /*
         * This line is why the method is in BlobBatchOperation and not BatchOperation.
         * The method requires a CloudBlobClient, not just a service client.
         */
        requestOptions = BlobRequestOptions.populateAndApplyDefaults(requestOptions, BlobType.UNSPECIFIED, client);

        return ExecutionEngine.executeWithRetry(
                client,
                this,
                this.batchImpl(client, requestOptions),
                requestOptions.getRetryPolicyFactory(),
                operationContext);
    }
}
