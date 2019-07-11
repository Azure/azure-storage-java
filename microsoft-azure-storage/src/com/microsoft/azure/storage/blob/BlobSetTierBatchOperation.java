package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.BatchSubResponse;
import com.microsoft.azure.storage.core.Utility;

public final class BlobSetTierBatchOperation extends BlobBatchOperation<CloudBlob, Void> {

    public void addSubOperation(CloudBlockBlob blockBlob, StandardBlobTier tier) {
        this.addSubOperation(blockBlob, tier, null /* options */);
    }

    public void addSubOperation(CloudBlockBlob blockBlob, StandardBlobTier tier, BlobRequestOptions options) {
        Utility.assertNotNull("blockBlob", blockBlob);
        Utility.assertNotNull("tier", tier);

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.BLOCK_BLOB, blockBlob.blobServiceClient);

        super.addSubOperation(blockBlob.uploadBlobTierImpl(tier.toString(), options), blockBlob);
    }

    public void addSubOperation(CloudPageBlob pageBlob, PremiumPageBlobTier tier) {
        this.addSubOperation(pageBlob, tier, null /* options */);
    }

    public void addSubOperation(CloudPageBlob pageBlob, PremiumPageBlobTier tier, BlobRequestOptions options) {
        Utility.assertNotNull("pageBlob", pageBlob);
        Utility.assertNotNull("tier", tier);

        options = BlobRequestOptions.populateAndApplyDefaults(options, BlobType.PAGE_BLOB, pageBlob.blobServiceClient);

        super.addSubOperation(pageBlob.uploadBlobTierImpl(tier.toString(), options), pageBlob);
    }

    @Override
    protected Void convertResponse(BatchSubResponse response) {
        return null;
    }
}
