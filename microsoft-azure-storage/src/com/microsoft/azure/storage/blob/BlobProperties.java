/**
 * Copyright Microsoft Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.azure.storage.blob;

import java.util.Date;

import com.microsoft.azure.storage.AccessCondition;

/**
 * Represents the system properties for a blob.
 */
public final class BlobProperties {

    /**
     * Represents the number of committed blocks on the append blob.
     */
    private Integer appendBlobCommittedBlockCount;
    
    /**
     * Represents the type of the blob.
     */
    private BlobType blobType = BlobType.UNSPECIFIED;

    /**
     * Represents the cache-control value stored for the blob.
     */
    private String cacheControl;

    /**
     * Represents the content-disposition value stored for the blob. If this field has not been set for the blob, the
     * field returns <code>null</code>.
     */
    private String contentDisposition;

    /**
     * Represents the content-encoding value stored for the blob. If this field has not been set for the blob, the field
     * returns <code>null</code>.
     */
    private String contentEncoding;

    /**
     * Represents the content-language value stored for the blob. If this field has not been set for the blob, the field
     * returns <code>null</code>.
     */
    private String contentLanguage;

    /**
     * Represents the content MD5 value stored for the blob.
     */
    private String contentMD5;

    /**
     * Represents the content type value stored for the blob. If this field has not been set for the blob, the field
     * returns <code>null</code>.
     */
    private String contentType;

    /**
     * Represents the state of the most recent or pending copy operation.
     */
    private CopyState copyState;

    /**
     * Represents the creation time for the blob, expressed as a UTC value.
     */
    private Date createdTime;

    /**
     * Represents the blob's ETag value.
     */
    private String etag;

    /**
     * Represents the last-modified time for the blob.
     */
    private Date lastModified;

    /**
     * Represents the blob's lease status.
     */
    private LeaseStatus leaseStatus = LeaseStatus.UNLOCKED;

    /**
     * Represents the blob's lease state.
     */
    private LeaseState leaseState;

    /**
     * Represents the blob's lease duration.
     */
    private LeaseDuration leaseDuration;

    /**
     * Represents the size, in bytes, of the blob.
     */
    private long length;
    
    /**
     * Represents the page blob's current sequence number.
     */
    private Long pageBlobSequenceNumber;
    
    /**
     * Represents the blob's server-side encryption status.
     */
    private boolean serverEncrypted;

    /**
     * Represents whether the blob is an incremental copy.
     */
    private boolean isIncrementalCopy;
    
    /**
     * Represents the premium page blob tier.
     */
    private PremiumPageBlobTier premiumPageBlobTier;

    /**
     * Represents the tier on a blob on a standard storage account.
     */
    private StandardBlobTier standardBlobTier;

    /**
     * Represents whether or not the blob tier is inferred.
     */
    private Boolean isBlobTierInferredTier;

    /**
     * Represents the last time the tier was changed.
     */
    private Date tierChangeTime;

    /**
     * Represents the rehydration status if the blob is being rehydrated.
     */
    private RehydrationStatus rehydrationStatus;

    /**
     * Indicates when the blob was deleted.
     */
    private Date deletedTime;

    /**
     * If deleted, this indicates how many days the blob will be retained before it is permanently deleted.
     */
    private Integer remainingRetentionDays;

    /**
     * Creates an instance of the <code>BlobProperties</code> class.
     */
    public BlobProperties() {
        // No op
    }

    /**
     * Creates an instance of the <code>BlobProperties</code> class by copying values from another instance of the
     * <code>BlobProperties</code> class.
     * 
     * @param other
     *        A <code>BlobProperties</code> object which represents the blob properties to copy.
     */
    public BlobProperties(final BlobProperties other) {
        this.appendBlobCommittedBlockCount = other.appendBlobCommittedBlockCount;
        this.blobType = other.blobType;
        this.cacheControl = other.cacheControl;
        this.contentDisposition = other.contentDisposition;
        this.contentEncoding = other.contentEncoding;
        this.contentLanguage = other.contentLanguage;
        this.contentMD5 = other.contentMD5;
        this.contentType = other.contentType;
        this.copyState = other.copyState;
        this.createdTime = other.createdTime;
        this.etag = other.etag;
        this.isBlobTierInferredTier = other.isBlobTierInferredTier;
        this.isIncrementalCopy = other.isIncrementalCopy;
        this.leaseStatus = other.leaseStatus;
        this.leaseState = other.leaseState;
        this.leaseDuration = other.leaseDuration;
        this.length = other.length;
        this.lastModified = other.lastModified;
        this.pageBlobSequenceNumber = other.pageBlobSequenceNumber;
        this.premiumPageBlobTier = other.premiumPageBlobTier;
        this.serverEncrypted = other.serverEncrypted;
        this.standardBlobTier = other.standardBlobTier;
        this.rehydrationStatus = other.rehydrationStatus;
        this.tierChangeTime = other.tierChangeTime;
        this.deletedTime = other.deletedTime;
        this.remainingRetentionDays = other.remainingRetentionDays;
    }

    /**
     * Creates an instance of the <code>BlobProperties</code> class.
     * 
     * @param type
     *        A <code>BlobType</code> object which represents the blob type.
     */
    public BlobProperties(final BlobType type) {
        this.blobType = type;
    }

    /**
     * If the blob is an append blob, gets the number of committed blocks.
     * 
     * @return A <code>Integer</code> value that represents the number of committed blocks.
     */
    public Integer getAppendBlobCommittedBlockCount() {
        return this.appendBlobCommittedBlockCount;
    }
    
    /**
     * Gets the blob type for the blob.
     * 
     * @return A {@link BlobType} value that represents the blob type.
     */
    public BlobType getBlobType() {
        return this.blobType;
    }

    /**
     * Gets the cache control value for the blob.
     * 
     * @return A <code>String</code> which represents the content cache control value for the blob.
     */
    public String getCacheControl() {
        return this.cacheControl;
    }

    /**
     * Gets the content disposition value for the blob.
     * 
     * @return A <code>String</code> which represents the content disposition, or <code>null</code> if content disposition has not been set
     *         on the blob.
     */
    public String getContentDisposition() {
        return this.contentDisposition;
    }

    /**
     * Gets the content encoding value for the blob.
     * 
     * @return A <code>String</code> which represents the content encoding, or <code>null</code> if content encoding has not been set
     *         on the blob.
     */
    public String getContentEncoding() {
        return this.contentEncoding;
    }

    /**
     * Gets the content language value for the blob.
     * 
     * @return A <code>String</code> which represents the content language, or <code>null</code> if content language has not been set on
     *         the blob.
     */
    public String getContentLanguage() {
        return this.contentLanguage;
    }

    /**
     * Gets the content MD5 value for the blob.
     * 
     * @return A <code>String</code> which represents the content MD5 value.
     */
    public String getContentMD5() {
        return this.contentMD5;
    }

    /**
     * Gets the content type value for the blob.
     * 
     * @return A <code>String</code> which represents the content type, or <code>null</code> if the content type has not be set for the blob.
     */
    public String getContentType() {
        return this.contentType;
    }

    /**
     * Gets the blob's copy state.
     * 
     * @return A {@link CopyState} object which represents the copy state of the blob.
     */
    public CopyState getCopyState() {
        return this.copyState;
    }

    /**
     * Gets the time when the blob was created.
     * @return A {@link java.util.Date} object which represents the time when the blob was created.
     */
    public Date getCreatedTime() {
        return this.createdTime;
    }

    /**
     * Gets the ETag value for the blob.
     * <p>
     * The ETag value is a unique identifier that is updated when a write operation is performed against the container.
     * It may be used to perform operations conditionally, providing concurrency control and improved efficiency.
     * <p>
     * The {@link AccessCondition#generateIfMatchCondition(String)} and
     * {@link AccessCondition#generateIfNoneMatchCondition(String)} methods take an ETag value and return an
     * {@link AccessCondition} object that may be specified on the request.
     * 
     * @return A <code>String</code> which represents the ETag value.
     */
    public String getEtag() {
        return this.etag;
    }

    /**
     * Gets the last modified time for the blob.
     * 
     * @return A {@link java.util.Date} object which represents the last modified time. 
     */
    public Date getLastModified() {
        return this.lastModified;
    }

    /**
     * Gets a value indicating if the tier of the blob has been inferred.
     *
     * @return A {@link java.lang.Boolean} object which represents if the blob tier was inferred.
     */
    public Boolean isBlobTierInferred() { return this.isBlobTierInferredTier; }

    /**
     * Gets a value indicating the last time the tier was changed on the blob.
     *
     * @return A {@link java.util.Date} object which represents the last time the tier was changed.
     */
    public Date getTierChangeTime() { return this.tierChangeTime; }

    /**
     * Gets the lease status for the blob.
     * 
     * @return A {@link LeaseStatus} object which represents the lease status. 
     */
    public LeaseStatus getLeaseStatus() {
        return this.leaseStatus;
    }

    /**
     * Gets the lease state for the blob.
     * 
     * @return A {@link LeaseState} object which represents the lease state. 
     */
    public LeaseState getLeaseState() {
        return this.leaseState;
    }

    /**
     * Gets the lease duration for the blob.
     * 
     * @return A {@link LeaseDuration} object which represents the lease duration. 
     */
    public LeaseDuration getLeaseDuration() {
        return this.leaseDuration;
    }

    /**
     * Gets the size, in bytes, of the blob.
     * 
     * @return A <code>long</code> which represents the length of the blob.
     */
    public long getLength() {
        return this.length;
    }
    
    /**
     * If the blob is a page blob, gets the page blob's current sequence number.
     * 
     * @return A <code>Long</code> containing the page blob's current sequence number.
     */
    public Long getPageBlobSequenceNumber() {
        return this.pageBlobSequenceNumber;
    }

    /**
     * If using a premium account and the blob is a page blob, gets the tier of the blob.
     * @return A {@link PremiumPageBlobTier} object which represents the tier of the blob
     * or <code>null</code> if the tier has not been set.
     */
    public PremiumPageBlobTier getPremiumPageBlobTier() {
        return this.premiumPageBlobTier;
    }

    /**
     * If using a standard account and the blob is a block blob, gets the tier of the blob.
     * @return A {@link StandardBlobTier} object which represents the tier of the blob
     * or <code>null</code> if the tier has not been set.
     */
    public StandardBlobTier getStandardBlobTier() {
        return this.standardBlobTier;
    }

    /**
     * The rehydration status if the blob is being rehydrated
     * and the tier of the blob once the rehydration from archive has completed.
     * @return
     */
    public RehydrationStatus getRehydrationStatus() { return this.rehydrationStatus; }

    /**
     * Gets the blob's server-side encryption status;
     * 
     * @return A <code>boolean</code> which specifies the blob's encryption status.
     */
    public boolean isServerEncrypted() {
        return serverEncrypted;
    }

    /**
     * Gets if the blob is an incremental copy
     * 
     * @return A <code>boolean</code> which specifies if the blob is an incremental copy.
     */
    public boolean isIncrementalCopy() {
        return this.isIncrementalCopy;
    }

    /**
     * Gets the time when the blob was deleted.
     * @return A {@link java.util.Date} object which represents the time when the blob was deleted. It returns null if the blob has not been deleted.
     */
    public Date getDeletedTime() {
        return this.deletedTime;
    }

    /**
     * Gets the number of days that the deleted blob will be kept by the service.
     * @return A <code>Integer</code> value that represents the number of days that the deleted blob will be kept by the service.
     */
    public Integer getRemainingRetentionDays() {
        return this.remainingRetentionDays;
    }

    /**
     * Sets the cache control value for the blob.
     * 
     * @param cacheControl
     *        A <code>String</code> which specifies the cache control value to set.
     */
    public void setCacheControl(final String cacheControl) {
        this.cacheControl = cacheControl;
    }

    /**
     * Sets the content disposition value for the blob.
     * 
     * @param contentDisposition
     *        A <code>String</code> which specifies the content disposition value to set.
     */
    public void setContentDisposition(final String contentDisposition) {
        this.contentDisposition = contentDisposition;
    }

    /**
     * Sets the content encoding value for the blob.
     * 
     * @param contentEncoding
     *        A <code>String</code> which specifies the content encoding value to set.
     */
    public void setContentEncoding(final String contentEncoding) {
        this.contentEncoding = contentEncoding;
    }

    /**
     * Sets the content language for the blob.
     * 
     * @param contentLanguage
     *        A <code>String</code> which specifies the content language value to set.
     */
    public void setContentLanguage(final String contentLanguage) {
        this.contentLanguage = contentLanguage;
    }

    /**
     * Sets the content MD5 value for the blob.
     * 
     * @param contentMD5
     *        A <code>String</code> which specifies the content MD5 value to set.
     */
    public void setContentMD5(final String contentMD5) {
        this.contentMD5 = contentMD5;
    }

    /**
     * Sets the content type value for the blob.
     * 
     * @param contentType
     *        A <code>String</code> which specifies the content type value to set.
     */
    public void setContentType(final String contentType) {
        this.contentType = contentType;
    }

    /**
     * If the blob is an append blob, sets the number of committed blocks.
     * 
     * @param appendBlobCommittedBlockCount
     *        A <code>Integer</code> value that represents the number of committed blocks.
     */
    protected void setAppendBlobCommittedBlockCount(final Integer appendBlobCommittedBlockCount) {
        this.appendBlobCommittedBlockCount  = appendBlobCommittedBlockCount;
    }
    
    /**
     * Sets the blob type.
     * 
     * @param blobType
     *        A {@link BlobType} object which specifies the blob type to set.
     */
    protected void setBlobType(final BlobType blobType) {
        this.blobType = blobType;
    }

    /**
     * Sets the copy state value for the blob
     * 
     * @param copyState
     *        A {@link CopyState} object which specifies the copy state value to set.
     */
    protected void setCopyState(final CopyState copyState) {
        this.copyState = copyState;
    }

    /**
     * Sets the createdTime value for the blob
     *
     * @param createdTime
     *        A <code>Date</code> which represents the time when the blob was created.
     */
    protected void setCreatedTime(final Date createdTime) {
        this.createdTime = createdTime;
    }

    /**
     * Sets the ETag value for the blob.
     * 
     * @param etag
     *        A <code>String</code> which specifies the ETag value to set.
     */
    protected void setEtag(final String etag) {
        this.etag = etag;
    }

    /**
     * Sets the last modified time for the blob.
     * 
     * @param lastModified
     *        A {@link java.util.Date} object which specifies the last modified time to set.
     */
    protected void setLastModified(final Date lastModified) {
        this.lastModified = lastModified;
    }

    /**
     * Sets the lease status for the blob.
     * 
     * @param leaseStatus
     *        A {@link LeaseStatus} object which specifies the lease status value to set.
     */
    protected void setLeaseStatus(final LeaseStatus leaseStatus) {
        this.leaseStatus = leaseStatus;
    }

    /**
     * Sets the lease state for the blob.
     * 
     * @param leaseState
     *        A {@link LeaseState} object which specifies the lease state value to set.
     */
    protected void setLeaseState(final LeaseState leaseState) {
        this.leaseState = leaseState;
    }

    /**
     * Sets the lease duration for the blob.
     * 
     * @param leaseDuration
     *        A {@link LeaseDuration} object which specifies the lease duration value to set.
     */
    protected void setLeaseDuration(final LeaseDuration leaseDuration) {
        this.leaseDuration = leaseDuration;
    }

    /**
     * Sets the content length, in bytes, for the blob.
     * 
     * @param length
     *        A <code>long</code> which specifies the length to set.
     */
    protected void setLength(final long length) {
        this.length = length;
    }
    
    /**
     * If the blob is a page blob, sets the blob's current sequence number.
     * 
     * @param pageBlobSequenceNumber
     *        A long containing the blob's current sequence number.
     */
    protected void setPageBlobSequenceNumber(final Long pageBlobSequenceNumber) {
        this.pageBlobSequenceNumber = pageBlobSequenceNumber;
    }

    /**
     * Sets the blob's server-side encryption status.
     * 
     * @param serverEncrypted
     *        A <code>boolean</code> which specifies the encryption status to set.
     */
    protected void setServerEncrypted(boolean serverEncrypted) {
        this.serverEncrypted = serverEncrypted;
    }

    /**
     * Sets whether the blob is an incremental copy.
     * @param isIncrementalCopy
     *        A <code>boolean</code> which specifies if the blob is an incremental copy.
     */
    protected void setIncrementalCopy(boolean isIncrementalCopy) {
        this.isIncrementalCopy = isIncrementalCopy;
    }

    /**
     * Sets the tier of the page blob. This is only supported for premium accounts.
     * @param premiumPageBlobTier
     *        A {@link PremiumPageBlobTier} object which represents the tier of the blob.
     */
    protected void setPremiumPageBlobTier(PremiumPageBlobTier premiumPageBlobTier) {
        this.premiumPageBlobTier = premiumPageBlobTier;
    }

    /**
     * Sets the tier of the block blob. This is only supported for standard storage accounts.
     * @param standardBlobTier
     *        A {@link StandardBlobTier} object which represents the tier of the blob.
     */
    protected void setStandardBlobTier(StandardBlobTier standardBlobTier) {
        this.standardBlobTier = standardBlobTier;
    }

    /**
     * Sets whether the blob tier is inferred.
     * @param isBlobTierInferredTier
     *      A {@link java.lang.Boolean} which specifies if the blob tier is inferred.
     */
    protected void setBlobTierInferred(Boolean isBlobTierInferredTier) {
        this.isBlobTierInferredTier = isBlobTierInferredTier;
    }

    /**
     * Sets the last time the tier was modified on the blob.
     * @param tierChangeTime
     *      A {@link java.util.Date} which specifies the last time the tier was modified.
     */
    protected void setTierChangeTime(Date tierChangeTime) {
        this.tierChangeTime = tierChangeTime;
    }

    /**
     * Sets the rehydration status of the blob.
     * @param rehydrationStatus
     *      A {@link RehydrationStatus} which specifies the rehydration status of the blob.
     */
    protected void setRehydrationStatus(RehydrationStatus rehydrationStatus) {
        this.rehydrationStatus = rehydrationStatus;
    }

    /**
     * Sets the time when the blob was deleted.
     * @param deletedTime
     *      A {@link java.util.Date} object which represents the time when the blob was deleted.
     */
    protected void setDeletedTime(Date deletedTime) {
        this.deletedTime = deletedTime;
    }

    /**
     * Sets the number days that the deleted blob will be kept by the service.
     * @param remainingRetentionDays
     *      A <code>Integer</code> value that represents the number days that the deleted blob will be kept by the service.
     */
    protected void setRemainingRetentionDays(Integer remainingRetentionDays) {
        this.remainingRetentionDays = remainingRetentionDays;
    }
}