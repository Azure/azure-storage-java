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
package com.microsoft.azure.storage.core;

import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.queue.*;

/**
 * RESERVED FOR INTERNAL USE. Contains helper methods for implementing shared access signatures.
 */
public class SharedAccessSignatureHelperForQueue {
    /**
     * Get the complete query builder for creating the Shared Access Signature query.
     *
     * @param policy
     *            The shared access policy for the shared access signature.
     * @param headers
     *            The optional header values to set for a blob or file accessed with this shared access signature.
     * @param groupPolicyIdentifier
     *            An optional identifier for the policy.
     * @param resourceType
     *            Either "b" for blobs, "c" for containers, "f" for files, or "s" for shares.
     * @param ipRange
     *            The range of IP addresses for the shared access signature.
     * @param protocols
     *            The Internet protocols for the shared access signature.
     * @param signature
     *            The signature to use.
     *
     * @return The finished query builder
     * @throws IllegalArgumentException
     * @throws StorageException
     */
    public static UriQueryBuilder generateSharedAccessSignature(
            final SharedAccessPolicy policy, final SharedAccessHeaders headers, final String groupPolicyIdentifier,
            final String resourceType, final IPRange ipRange, final SharedAccessProtocols protocols, final String signature)
            throws StorageException {

        Utility.assertNotNullOrEmpty("resourceType", resourceType);

        return generateSharedAccessSignatureHelper(policy, null /* startPartitionKey */, null /* startRowKey */,
                null /* endPartitionKey */, null /* endRowKey */, groupPolicyIdentifier, resourceType, ipRange,
                protocols, null /* tableName */, signature, headers);
    }

    /**
     * Get the signature hash embedded inside the Shared Access Signature for the blob or file service.
     *
     * @param policy
     *            The shared access policy to hash.
     * @param headers
     *            The optional header values to set for a blob or file accessed with this shared access signature.
     * @param accessPolicyIdentifier
     *            An optional identifier for the policy.
     * @param resourceName
     *            The resource name.
     * @param ipRange
     *            The range of IP addresses to hash.
     * @param protocols
     *            The Internet protocols to hash.
     * @param client
     *            The ServiceClient associated with the object.
     *
     * @return The signature hash embedded inside the Shared Access Signature.
     * @throws InvalidKeyException
     * @throws StorageException
     */
    public static String generateSharedAccessSignatureHash(final SharedAccessPolicy policy,
                                                           SharedAccessHeaders headers, final String accessPolicyIdentifier, final String resourceName,
                                                           final IPRange ipRange, final SharedAccessProtocols protocols, final ServiceClient client)
            throws InvalidKeyException, StorageException {

        String stringToSign = generateSharedAccessSignatureStringToSign(
                policy, resourceName, ipRange, protocols, accessPolicyIdentifier);

        String cacheControl = null;
        String contentDisposition = null;
        String contentEncoding = null;
        String contentLanguage = null;
        String contentType = null;
        if (headers != null) {
            cacheControl = headers.getCacheControl();
            contentDisposition = headers.getContentDisposition();
            contentEncoding = headers.getContentEncoding();
            contentLanguage = headers.getContentLanguage();
            contentType = headers.getContentType();
        }

        stringToSign = String.format("%s\n%s\n%s\n%s\n%s\n%s", stringToSign,
                cacheControl == null ? Constants.EMPTY_STRING : cacheControl,
                contentDisposition == null ? Constants.EMPTY_STRING : contentDisposition,
                contentEncoding == null ? Constants.EMPTY_STRING : contentEncoding,
                contentLanguage == null ? Constants.EMPTY_STRING : contentLanguage,
                contentType == null ? Constants.EMPTY_STRING : contentType);

        return SharedAccessSignatureHelperForAccount.generateSharedAccessSignatureHashHelper(stringToSign, client.getCredentials());
    }

    /**
     * Helper to add a name/value pair to a <code>UriQueryBuilder</code> if the value is not null or empty.
     *
     * @param builder
     *            The builder to add to.
     * @param name
     *            The name to add.
     * @param val
     *            The value to add if not null or empty.
     *
     * @throws StorageException
     *            An exception representing any error which occurred during the operation.
     */
    private static void addIfNotNullOrEmpty(UriQueryBuilder builder, String name, String val) throws StorageException {
        if (!Utility.isNullOrEmpty(val)) {
            builder.add(name, val);
        }
    }

    /**
     * Get the complete query builder for creating the Shared Access Signature query.
     *
     * @param policy
     *            A {@link SharedAccessPolicy} containing the permissions for the SAS.
     * @param startPartitionKey
     *            The start partition key for a shared access signature URI.
     * @param startRowKey
     *            The start row key for a shared access signature URI.
     * @param endPartitionKey
     *            The end partition key for a shared access signature URI.
     * @param endRowKey
     *            The end row key for a shared access signature URI.
     * @param accessPolicyIdentifier
     *            An optional identifier for the policy.
     * @param resourceType
     *            The resource type for a shared access signature URI.
     * @param ipRange
     *            The range of IP addresses for the shared access signature.
     * @param protocols
     *            The Internet protocols for the shared access signature.
     * @param tableName
     *            The table name.
     * @param signature
     *            The signature hash.
     * @param headers
     *            Optional blob or file headers.
     *
     * @return The finished query builder.
     * @throws StorageException
     *            An exception representing any error which occurred during the operation.
     */
    static UriQueryBuilder generateSharedAccessSignatureHelper(
            final SharedAccessPolicy policy, final String startPartitionKey, final String startRowKey,
            final String endPartitionKey, final String endRowKey, final String accessPolicyIdentifier,
            final String resourceType, final IPRange ipRange, final SharedAccessProtocols protocols,
            final String tableName, final String signature, final SharedAccessHeaders headers)
            throws StorageException {

        Utility.assertNotNull("signature", signature);

        String permissions = null;
        Date startTime = null;
        Date expiryTime = null;
        if (policy != null) {
            permissions = policy.permissionsToString();
            startTime = policy.getSharedAccessStartTime();
            expiryTime = policy.getSharedAccessExpiryTime();
        }

        final UriQueryBuilder builder = new UriQueryBuilder();

        builder.add(Constants.QueryConstants.SIGNED_VERSION, QueueConstants.TARGET_STORAGE_VERSION);
        addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_PERMISSIONS, permissions);

        final String startString = Utility.getUTCTimeOrEmpty(startTime);
        addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_START, startString);

        final String stopString = Utility.getUTCTimeOrEmpty(expiryTime);
        addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_EXPIRY, stopString);

        addIfNotNullOrEmpty(builder, Constants.QueryConstants.START_PARTITION_KEY, startPartitionKey);
        addIfNotNullOrEmpty(builder, Constants.QueryConstants.START_ROW_KEY, startRowKey);
        addIfNotNullOrEmpty(builder, Constants.QueryConstants.END_PARTITION_KEY, endPartitionKey);
        addIfNotNullOrEmpty(builder, Constants.QueryConstants.END_ROW_KEY, endRowKey);

        addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_IDENTIFIER, accessPolicyIdentifier);
        addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_RESOURCE, resourceType);
        addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNED_IP, ipRange != null ? ipRange.toString() : null);
        addIfNotNullOrEmpty(
                builder, Constants.QueryConstants.SIGNED_PROTOCOLS, protocols != null ? protocols.toString() : null);

        addIfNotNullOrEmpty(builder, Constants.QueryConstants.SAS_TABLE_NAME, tableName);

        if (headers != null) {
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.CACHE_CONTROL, headers.getCacheControl());
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_TYPE, headers.getContentType());
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_ENCODING, headers.getContentEncoding());
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_LANGUAGE, headers.getContentLanguage());
            addIfNotNullOrEmpty(builder, Constants.QueryConstants.CONTENT_DISPOSITION, headers.getContentDisposition());
        }

        addIfNotNullOrEmpty(builder, Constants.QueryConstants.SIGNATURE, signature);

        return builder;
    }

    /**
     * Get the signature hash embedded inside the Shared Access Signature.
     *
     * @param policy
     *            A {@link SharedAccessPolicy} containing the permissions for the SAS.
     * @param resource
     *            The canonical resource (or resource type) string, unescaped.
     * @param ipRange
     *            The range of IP addresses to hash.
     * @param protocols
     *            The Internet protocols to hash.
     * @param headers
     *            The optional header values to set for a blob or file accessed with this shared access signature.
     * @param accessPolicyIdentifier
     *            An optional identifier for the policy.
     *
     * @return The signature hash embedded inside the Shared Access Signature.
     *
     * @throws InvalidKeyException
     * @throws StorageException
     */
    static String generateSharedAccessSignatureStringToSign(
            final SharedAccessPolicy policy, final String resource, final IPRange ipRange,
            final SharedAccessProtocols protocols, final String accessPolicyIdentifier)
            throws InvalidKeyException, StorageException {

        Utility.assertNotNullOrEmpty("resource", resource);

        String permissions = null;
        Date startTime = null;
        Date expiryTime = null;

        if (policy != null) {
            permissions = policy.permissionsToString();
            startTime = policy.getSharedAccessStartTime();
            expiryTime = policy.getSharedAccessExpiryTime();
        }

        String stringToSign = String.format("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s",
                permissions == null ? Constants.EMPTY_STRING : permissions,
                Utility.getUTCTimeOrEmpty(startTime), Utility.getUTCTimeOrEmpty(expiryTime), resource,
                accessPolicyIdentifier == null ? Constants.EMPTY_STRING : accessPolicyIdentifier,
                ipRange == null ? Constants.EMPTY_STRING : ipRange.toString(),
                protocols == null ? Constants.EMPTY_STRING : protocols.toString(),
                QueueConstants.TARGET_STORAGE_VERSION);

        return stringToSign;
    }



    /**
     * Get the complete query builder for creating the Shared Access Signature query.
     *
     * @param policy                The shared access policy for the shared access signature.
     * @param groupPolicyIdentifier An optional identifier for the policy.
     * @param ipRange               The range of IP addresses for the shared access signature.
     * @param protocols             The Internet protocols for the shared access signature.
     * @param signature             The signature to use.
     * @return The finished query builder
     * @throws IllegalArgumentException
     * @throws StorageException
     */
    public static UriQueryBuilder generateSharedAccessSignature(
            final SharedAccessQueuePolicy policy, final String groupPolicyIdentifier, final IPRange ipRange,
            final SharedAccessProtocols protocols, final String signature)
            throws StorageException {

        return SharedAccessSignatureHelperForQueue.generateSharedAccessSignatureHelper(policy, null /* startPartitionKey */, null /* startRowKey */,
                null /* endPartitionKey */, null /* endRowKey */, groupPolicyIdentifier, null /* resourceType */,
                ipRange, protocols, null /* tableName */, signature, null /* headers */);
    }

    /**
     * Get the signature hash embedded inside the Shared Access Signature for queue service.
     *
     * @param policy
     *            The shared access policy to hash.
     * @param accessPolicyIdentifier
     *            An optional identifier for the policy.
     * @param resourceName
     *            The resource name.
     * @param ipRange
     *            The range of IP addresses to hash.
     * @param protocols
     *            The Internet protocols to hash.
     * @param client
     *            The ServiceClient associated with the object.
     *
     * @return The signature hash embedded inside the Shared Access Signature.
     * @throws InvalidKeyException
     * @throws StorageException
     */
    public static String generateSharedAccessSignatureHash(
            final SharedAccessQueuePolicy policy, final String accessPolicyIdentifier, final String resourceName,
            final IPRange ipRange, final SharedAccessProtocols protocols, final ServiceClient client)
            throws InvalidKeyException, StorageException {

        final String stringToSign = SharedAccessSignatureHelperForQueue.generateSharedAccessSignatureStringToSign(
                policy, resourceName, ipRange, protocols, accessPolicyIdentifier);

        return SharedAccessSignatureHelperForAccount.generateSharedAccessSignatureHashHelper(stringToSign, client.getCredentials());
    }

    /**
     * Private Default Ctor.
     */
    private SharedAccessSignatureHelperForQueue() {
        // No op
    }
}
