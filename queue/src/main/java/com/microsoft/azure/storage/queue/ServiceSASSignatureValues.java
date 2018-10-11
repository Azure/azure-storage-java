/*
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
package com.microsoft.azure.storage.queue;

import java.security.InvalidKeyException;
import java.time.OffsetDateTime;

/**
 * ServiceSASSignatureValues is used to generate a Shared Access Signature (SAS) for an Azure Storage service. Once
 * all the values here are set appropriately, call generateSASQueryParameters to obtain a representation of the SAS
 * which can actually be applied to blob urls. Note: that both this class and {@link SASQueryParameters} exist because
 * the former is mutable and a logical representation while the latter is immutable and used to generate actual REST
 * requests.
 * <p>
 * Please see <a href=https://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1>here</a>
 * for more conceptual information on SAS.
 * <p>
 * Please see <a href=https://docs.microsoft.com/en-us/rest/api/storageservices/constructing-a-service-sas>here </a> for
 * more details on each value, including which are required.
 */
public final class ServiceSASSignatureValues {


    private String version = Constants.HeaderConstants.TARGET_STORAGE_VERSION;

    private SASProtocol protocol;

    private OffsetDateTime startTime;

    private OffsetDateTime expiryTime;

    private String permissions;

    private IPRange ipRange;

    private String queueName;

    private String identifier;

    /**
     * Creates an object with empty values for all fields.
     */
    public ServiceSASSignatureValues() {
    }

    /**
     * The version of the service this SAS will target. If not specified, it will default to the version targeted by the
     * library.
     */
    public String version() {
        return version;
    }

    /**
     * The version of the service this SAS will target. If not specified, it will default to the version targeted by the
     * library.
     */
    public ServiceSASSignatureValues withVersion(String version) {
        this.version = version;
        return this;
    }

    /**
     * {@link SASProtocol}
     */
    public SASProtocol protocol() {
        return protocol;
    }

    /**
     * {@link SASProtocol}
     */
    public ServiceSASSignatureValues withProtocol(SASProtocol protocol) {
        this.protocol = protocol;
        return this;
    }

    /**
     * When the SAS will take effect.
     */
    public OffsetDateTime startTime() {
        return startTime;
    }

    /**
     * When the SAS will take effect.
     */
    public ServiceSASSignatureValues withStartTime(OffsetDateTime startTime) {
        this.startTime = startTime;
        return this;
    }

    /**
     * The time after which the SAS will no longer work.
     */
    public OffsetDateTime expiryTime() {
        return expiryTime;
    }

    /**
     * The time after which the SAS will no longer work.
     */
    public ServiceSASSignatureValues withExpiryTime(OffsetDateTime expiryTime) {
        this.expiryTime = expiryTime;
        return this;
    }

    /**
     * Please refer to {@link QueueSASPermission} depending on the resource
     * being accessed for help constructing the permissions string.
     */
    public String permissions() {
        return permissions;
    }

    /**
     * Please refer to {@link QueueSASPermission} depending on the resource
     * being accessed for help constructing the permissions string.
     */
    public ServiceSASSignatureValues withPermissions(String permissions) {
        this.permissions = permissions;
        return this;
    }

    /**
     * {@link IPRange}
     */
    public IPRange ipRange() {
        return ipRange;
    }

    /**
     * {@link IPRange}
     */
    public ServiceSASSignatureValues withIpRange(IPRange ipRange) {
        this.ipRange = ipRange;
        return this;
    }

    /**
     * The name of the queue the SAS user may access.
     */
    public String queueName() {
        return queueName;
    }

    /**
     * The name of the queue the SAS user may access.
     */
    public ServiceSASSignatureValues withQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    /**
     * The name of the access policy on the queue this SAS references if any. Please see
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/establishing-a-stored-access-policy">here</a>
     * for more information.
     */
    public String identifier() {
        return identifier;
    }

    /**
     * The name of the access policy on the queue this SAS references if any. Please see
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/establishing-a-stored-access-policy">here</a>
     * for more information.
     */
    public ServiceSASSignatureValues withIdentifier(String identifier) {
        this.identifier = identifier;
        return this;
    }

    /**
     * Uses an account's shared key credential to sign these signature values to produce the proper SAS query
     * parameters.
     *
     * @param sharedKeyCredentials
     *         A {@link SharedKeyCredentials} object used to sign the SAS values.
     *
     * @return {@link SASQueryParameters}
     */
    public SASQueryParameters generateSASQueryParameters(SharedKeyCredentials sharedKeyCredentials) {
        if (sharedKeyCredentials == null) {
            throw new IllegalArgumentException("SharedKeyCredentials cannot be null.");
        }

        String verifiedPermissions = null;
        if (this.permissions != null) {
            verifiedPermissions = QueueSASPermission.parse(this.permissions).toString();
        }

        // Signature is generated on the un-url-encoded values.
        final String stringToSign = stringToSign(verifiedPermissions, sharedKeyCredentials);

        String signature = null;
        try {
            signature = sharedKeyCredentials.computeHmac256(stringToSign);
        } catch (InvalidKeyException e) {
            throw new Error(e); // The key should have been validated by now. If it is no longer valid here, we fail.
        }

        return new SASQueryParameters(this.version, null, null,
                this.protocol, this.startTime, this.expiryTime, this.ipRange, this.identifier,
                this.permissions, signature);
    }

    private String getCanonicalName(String accountName) {
        // Queue: "/queue/account/queuename"
        StringBuilder canonicalName = new StringBuilder("/queue");
        canonicalName.append('/').append(accountName).append('/').append(this.queueName);

        return canonicalName.toString();
    }

    private String stringToSign(final String verifiedPermissions,
            final SharedKeyCredentials sharedKeyCredentials) {
        return String.join("\n",
                verifiedPermissions == null ? "" : verifiedPermissions,
                this.startTime == null ? "" : Utility.ISO8601UTCDateFormatter.format(this.startTime),
                this.expiryTime == null ? "" : Utility.ISO8601UTCDateFormatter.format(this.expiryTime),
                getCanonicalName(sharedKeyCredentials.getAccountName()),
                this.identifier == null ? "" : this.identifier,
                this.ipRange == null ? IPRange.DEFAULT.toString() : this.ipRange.toString(),
                this.protocol == null ? "" : protocol.toString(),
                this.version
        );
    }
}
