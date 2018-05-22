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
 * Represents the system properties for a container.
 */
public final class BlobContainerProperties {

    /**
     * Represents the ETag value for the container.
     */
    private String etag;

    /**
     * Represents whether the container has an immutability policy.
     */
    private Boolean hasImmutabilityPolicy;

    /**
     * Represents whether the container has a legal hold.
     */
    private Boolean hasLegalHold;

    /**
     * Represents the container's last-modified time.
     */
    private Date lastModified;

    /**
     * Represents the container's lease status.
     */
    private LeaseStatus leaseStatus;

    /**
     * Represents the container's lease state.
     */
    private LeaseState leaseState;

    /**
     * Represents the container's lease duration.
     */
    private LeaseDuration leaseDuration;

    /**
     * Represents the level of public access that is allowed on the container.
     */
    private BlobContainerPublicAccessType publicAccess;

    /**
     * Gets the ETag value of the container.
     * <p>
     * The ETag value is a unique identifier that is updated when a write operation is performed against the container.
     * It may be used to perform operations conditionally, providing concurrency control and improved efficiency.
     * <p>
     * The {@link AccessCondition#generateIfMatchCondition(String)} and
     * {@link AccessCondition#generateIfNoneMatchCondition(String)} methods take an ETag value and return an
     * {@link AccessCondition} object that may be specified on the request.
     *
     * @return A <code>String</code> which represents the ETag.
     */
    public String getEtag() {
        return this.etag;
    }

    /**
     * Gets the hasImmutabilityPolicy value of the container.
     *
     * @return A <code>Boolean</code> which represents the hasImmutabilityPolicy value.
     */
    public Boolean hasImmutabilityPolicy() {
        return this.hasImmutabilityPolicy;
    }

    /**
     * Gets the hasLegalHold value of the container.
     *
     * @return A <code>Boolean</code> which represents the hasLegalHold value.
     */
    public Boolean hasLegalHold() {
        return this.hasLegalHold;
    }

    /**
     * Gets the last modified time on the container.
     *
     * @return A <code>java.util.Date</code> object which represents the last modified time.
     */
    public Date getLastModified() {
        return this.lastModified;
    }

    /**
     * Gets the lease status of the container.
     *
     * @return A <code>{@link LeaseStatus}</code> object which represents the lease status of the container.
     */
    public LeaseStatus getLeaseStatus() {
        return this.leaseStatus;
    }

    /**
     * Gets the lease state of the container.
     *
     * @return A <code>{@link LeaseState}</code> object which represents the lease state of the container.
     */
    public LeaseState getLeaseState() {
        return this.leaseState;
    }

    /**
     * Gets the lease duration of the container.
     *
     * @return A <code>{@link LeaseDuration}</code> object which represents the lease duration of the container.
     */
    public LeaseDuration getLeaseDuration() {
        return this.leaseDuration;
    }

    /**
     * Gets the public access level for the container.
     * This field should only be set using the container's {@link CloudBlobContainer#create(BlobContainerPublicAccessType,
     * BlobRequestOptions, com.microsoft.azure.storage.OperationContext) create} method or
     * {@link CloudBlobContainer#uploadPermissions(BlobContainerPermissions) uploadPermissions} method.
     *
     * @return A <code>{@link BlobContainerPublicAccessType}</code> that specifies the level of public access
     * that is allowed on the container.
     */
    public BlobContainerPublicAccessType getPublicAccess() {
        return this.publicAccess;
    }

    /**
     * Sets the ETag value on the container.
     *
     * @param etag
     *        A <code>String</code> which represents the ETag to set.
     */
    protected void setEtag(final String etag) {
        this.etag = etag;
    }

    /**
     * Sets the hasImmutabilityPolicy value on the container.
     *
     * @param hasImmutabilityPolicy
     *        A <code>Boolean</code> which represents the hasImmutabilityProperty value to set.
     */
    protected void setHasImmutabilityPolicy(final Boolean hasImmutabilityPolicy) {
        this.hasImmutabilityPolicy = hasImmutabilityPolicy;
    }

    /**
     * Sets the hasLegalHold value on the container.
     *
     * @param hasLegalHold
     *        A <code>Boolean</code> which represents the hasLegalHold value to set.
     */
    protected void setHasLegalHold(final Boolean hasLegalHold) {
        this.hasLegalHold= hasLegalHold;
    }

    /**
     * Sets the last modified time on the container.
     *
     * @param lastModified
     *        A <code>java.util.Date</code> object which represents the last modified time to set.
     */
    protected void setLastModified(final Date lastModified) {
        this.lastModified = lastModified;
    }

    /**
     * Sets the lease status on the container.
     *
     * @param leaseStatus
     *        A <code>{@link LeaseStatus}</code> object which represents the lease status of the container.
     */
    protected void setLeaseStatus(final LeaseStatus leaseStatus) {
        this.leaseStatus = leaseStatus;
    }

    /**
     * Sets the lease status on the container.
     *
     * @param leaseState
     *        A <code>{@link LeaseState}</code> object which represents the lease state of the container.
     */
    protected void setLeaseState(final LeaseState leaseState) {
        this.leaseState = leaseState;
    }

    /**
     * Sets the lease duration on the container.
     *
     * @param leaseDuration
     *        A <code>{@link LeaseDuration}</code> object which represents the lease duration of the container.
     */
    protected void setLeaseDuration(final LeaseDuration leaseDuration) {
        this.leaseDuration = leaseDuration;
    }

    /**
     * Sets the public access level on the container.
     * This should only be set using the container's {@link CloudBlobContainer#create(BlobContainerPublicAccessType,
     * BlobRequestOptions, com.microsoft.azure.storage.OperationContext) create} method or
     * {@link CloudBlobContainer#uploadPermissions(BlobContainerPermissions) uploadPermissions} method.
     *
     * @param publicAccess
     *            A <code>{@link BlobContainerPublicAccessType}</code> object
     *            which represents the public access level on the container.
     */
    protected void setPublicAccess(
            final BlobContainerPublicAccessType publicAccess) {
        this.publicAccess = publicAccess;
    }
}
