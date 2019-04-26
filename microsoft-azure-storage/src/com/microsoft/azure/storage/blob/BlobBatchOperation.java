package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.BatchOperation;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.core.ExecutionEngine;
import com.microsoft.azure.storage.core.StorageRequest;

import java.net.HttpURLConnection;

public abstract class BlobBatchOperation<R> extends BatchOperation<CloudBlobClient, CloudBlob, R> {

    final Iterable<R> execute(CloudBlobClient client, BlobRequestOptions requestOptions, OperationContext operationContext)
            throws StorageException {
        if (operationContext == null) {
            operationContext = new OperationContext();
        }

        operationContext.initialize();
        requestOptions = BlobRequestOptions.populateAndApplyDefaults(requestOptions, BlobType.UNSPECIFIED, client); // TODO try and specify blob type

        return ExecutionEngine.executeWithRetry(
                client,
                this,
                this.blobBatchImpl(client, requestOptions),
                requestOptions.getRetryPolicyFactory(),
                operationContext);
    }

    protected final StorageRequest<CloudBlobClient, BlobBatchOperation<R>, Iterable<R>> blobBatchImpl(
            CloudBlobClient client, BlobRequestOptions requestOptions) {

        return new StorageRequest<CloudBlobClient, BlobBatchOperation<R>, Iterable<R>>(requestOptions, client.getStorageUri()) {
            @Override
            public HttpURLConnection buildRequest(CloudBlobClient client, BlobBatchOperation<R> parentObject, OperationContext context) throws Exception {
                return null;
            }

            @Override
            public void signRequest(HttpURLConnection connection, CloudBlobClient client, OperationContext context) throws Exception {
                StorageRequest.signBlobQueueAndFileRequest(connection, client, 0L, context);
            }

            @Override
            public Iterable<R> preProcessResponse(BlobBatchOperation<R> parentObject, CloudBlobClient client, OperationContext context) throws Exception {
                return null;
            }
        };
    }
}
