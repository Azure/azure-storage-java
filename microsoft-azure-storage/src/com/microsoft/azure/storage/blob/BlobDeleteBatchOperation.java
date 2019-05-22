package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.BatchSubResponse;
import com.microsoft.azure.storage.core.Utility;

public class BlobDeleteBatchOperation extends BlobBatchOperation<CloudBlob, Void> {

    public void addSubOperation(CloudBlob blob) {
        this.addSubOperation(blob, DeleteSnapshotsOption.NONE, null /* accessCondition */, null /* options */);
    }

    public void addSubOperation(CloudBlob blob, final DeleteSnapshotsOption deleteSnapshotsOption,
                                final AccessCondition accessCondition, BlobRequestOptions options) {
        Utility.assertNotNull("blob", blob);
        Utility.assertNotNull("deleteSnapshotsOption", deleteSnapshotsOption);

        options = BlobRequestOptions.populateAndApplyDefaults(options, blob.properties.getBlobType(), blob.blobServiceClient);

        super.addSubOperation(blob.deleteImpl(deleteSnapshotsOption, accessCondition, options), blob);
    }

    @Override
    protected Void convertResponse(BatchSubResponse response) {
        return null;
    }
}
