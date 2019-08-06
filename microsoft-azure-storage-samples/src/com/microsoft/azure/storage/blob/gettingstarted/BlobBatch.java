package com.microsoft.azure.storage.blob.gettingstarted;

import com.microsoft.azure.storage.BatchException;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.util.Utility;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class BlobBatch {
    private static final String SAMPLE_NAME = "BlobBatch";

    public static void main(String[] args) throws URISyntaxException, InvalidKeyException, StorageException {
        Utility.printSampleStartInfo(SAMPLE_NAME);

        // Setup the cloud storage account.
        CloudStorageAccount account = CloudStorageAccount.parse(Utility.storageConnectionString);

        // Create a blob service client
        CloudBlobClient blobClient = account.createCloudBlobClient();

        // Get a reference to a container
        // The container name must be lower case
        // Append a random UUID to the end of the container name so that
        // this sample can be run more than once in quick succession.
        CloudBlobContainer container = blobClient.getContainerReference("blobbasicscontainer"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            // create the container
            container.create();

            // add some blobs
            final int NUM_BLOBS = 3;
            List<CloudBlob> blobs = new ArrayList<>();
            for (int i = 0; i < NUM_BLOBS; i++) {
                // blobs have more relaxed naming constraints than containers
                CloudBlockBlob blob = container.getBlockBlobReference("blobtobatchdelete" + UUID.randomUUID());
                blob.uploadText("Sample data.");
                blobs.add(blob);

                System.out.println(String.format("Created blob %s", blob.getName()));
            }

            // create a blob object on client, but don't upload it to the service, to cause a bad request in the batch
            // comment this line out to get a success state
            blobs.add(container.getBlockBlobReference("blobtobatchdelete" + UUID.randomUUID()));

            // assemble batch request, in this case: delete
            BlobDeleteBatchOperation batch = new BlobDeleteBatchOperation();
            for (CloudBlob blob : blobs) {
                batch.addSubOperation(blob);

                System.out.println(String.format("Added delete request for blob %s", blob.getName()));
            }

            try {
                // send the batch request
                Map<CloudBlob, Void> batchResponse = container.getServiceClient().executeBatch(batch);

                // for each blob, view the result
                // Delete returns void, so its batch response maps to Void, but other requests may return data to process
                for (CloudBlob blob : blobs) {
                    Void result = batchResponse.get(blob);

                    System.out.println(String.format("Result from deleting blob %s: %s", blob.getName(), result));
                }
            }

            // when one or more requests in the batch fail
            catch (BatchException e) {
                /*
                 * Subclasses of java.lang.Throwable cannot be generic, so the collections of successful and
                 * unsuccessful responses cannot be typed correctly on the class. However, the types will directly
                 * relate to the batched operation. Meaning: for a BlobBatchOperation<P, R>,
                 * e.getSuccessfulResponses() can be safely cast to Map<P, R>, and e.getExceptions() can be
                 * safely cast to Map<P, StorageException>. BlobDeleteBatchOperation extends
                 * BlobBatchOperation<CloudBlob, Void> so we cast as follows.
                 */
                Map<CloudBlob, Void> successes = (Map<CloudBlob, Void>) e.getSuccessfulResponses();
                Map<CloudBlob, StorageException> failures = (Map<CloudBlob, StorageException>) e.getExceptions();

                // handle successes
                for (Map.Entry<CloudBlob, Void> entry : successes.entrySet()) {
                    System.out.println(String.format("Result from deleting blob %s: %s",
                            entry.getKey().getName(), entry.getValue()));
                }

                // handle failures
                for (Map.Entry<CloudBlob, StorageException> entry : failures.entrySet()) {
                    System.out.println(String.format("Failed to delete blob %s. Exception: %s",
                            entry.getKey().getName(), entry.getValue().getErrorCode()));
                }
            }
        }
        catch (Throwable t) {
            Utility.printException(t);
        }
        finally {
            // clean up sample
            container.deleteIfExists();
        }

        Utility.printSampleCompleteInfo(SAMPLE_NAME);
    }
}
