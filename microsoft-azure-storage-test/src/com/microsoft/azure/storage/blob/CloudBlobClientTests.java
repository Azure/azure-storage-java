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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;

import com.microsoft.azure.storage.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.microsoft.azure.storage.core.SR;
import com.microsoft.azure.storage.TestRunners.CloudTests;
import com.microsoft.azure.storage.TestRunners.DevFabricTests;
import com.microsoft.azure.storage.TestRunners.DevStoreTests;

import static org.junit.Assert.*;

/**
 * Blob Client Tests
 */
public class CloudBlobClientTests {
    /**
     *
     * @throws StorageException
     * @throws URISyntaxException
     */
    @Test
    @Category({ DevFabricTests.class, DevStoreTests.class, CloudTests.class })
    public void testListContainers() throws StorageException, URISyntaxException {
        CloudBlobClient bClient = BlobTestHelper.createCloudBlobClient();
        ArrayList<String> containerList = new ArrayList<String>();
        String prefix = UUID.randomUUID().toString();
        for (int i = 0; i < 30; i++) {
            containerList.add(prefix + i);
            bClient.getContainerReference(prefix + i).create();
        }

        int count = 0;
        for (final CloudBlobContainer container : bClient.listContainers(prefix)) {
            assertEquals(CloudBlobContainer.class, container.getClass());
            count++;
        }
        assertEquals(30, count);

        ResultContinuation token = null;
        do {

            ResultSegment<CloudBlobContainer> segment = bClient.listContainersSegmented(prefix,
                    ContainerListingDetails.ALL, 15, token, null, null);

            for (final CloudBlobContainer container : segment.getResults()) {
                container.downloadAttributes();
                assertEquals(CloudBlobContainer.class, container.getClass());
                assertNotNull(container.getProperties().hasImmutabilityPolicy());
                assertNotNull(container.getProperties().hasLegalHold());
                assertFalse(container.getProperties().hasImmutabilityPolicy());
                assertFalse(container.getProperties().hasLegalHold());
                containerList.remove(container.getName());
            }

            token = segment.getContinuationToken();
        } while (token != null);

        assertEquals(0, containerList.size());
    }

    /**
     * Try to list the containers to ensure maxResults validation is working.
     *
     * @throws StorageException
     * @throws URISyntaxException
     */
    @Test
    @Category({ DevFabricTests.class, DevStoreTests.class, CloudTests.class })
    public void testListContainersMaxResultsValidation()
            throws StorageException, URISyntaxException {
        CloudBlobClient bClient = BlobTestHelper.createCloudBlobClient();
        String prefix = UUID.randomUUID().toString();

        // Validation should cause each of these to fail.
        for(int i = 0; i >= -2; i--) {
            try {
                bClient.listContainersSegmented(
                        prefix, ContainerListingDetails.ALL, i, null, null, null);
                fail();
            }
            catch (IllegalArgumentException e) {
                assertTrue(String.format(SR.PARAMETER_SHOULD_BE_GREATER_OR_EQUAL, "maxResults", 1)
                        .equals(e.getMessage()));
            }
        }
        assertNotNull(bClient.listContainersSegmented("thereshouldntbeanycontainersswiththisprefix"));
    }

    /**
     * Fetch result segments and ensure pageSize is null when unspecified and will cap at 5000.
     *
     * @throws StorageException
     * @throws URISyntaxException
     */
    @Test
    @Category({ DevFabricTests.class, DevStoreTests.class, CloudTests.class })
    public void testListContainersResultSegment()
            throws StorageException, URISyntaxException {
        CloudBlobClient bClient = BlobTestHelper.createCloudBlobClient();

        ResultSegment<CloudBlobContainer> segment1 = bClient.listContainersSegmented();
        assertNotNull(segment1);
        assertNull(segment1.getPageSize());

        ResultSegment<CloudBlobContainer> segment2 = bClient.listContainersSegmented(null,
                ContainerListingDetails.ALL, 9001, null, null, null);
        assertNotNull(segment2);
        assertNotNull(segment2.getPageSize());
        assertEquals(5000, segment2.getPageSize().intValue());
    }

    /**
     * List containers and fetch attributes with public access
     *
     * @throws StorageException
     * @throws URISyntaxException
     */
    @Test
    @Category({ DevFabricTests.class, DevStoreTests.class, CloudTests.class })
    public void testCreateContainerWithPublicAccess() throws StorageException, URISyntaxException {
        CloudBlobClient bClient = BlobTestHelper.createCloudBlobClient();
        BlobContainerPublicAccessType[] accessTypes = { BlobContainerPublicAccessType.CONTAINER, 
                BlobContainerPublicAccessType.OFF, BlobContainerPublicAccessType.BLOB };
        for(BlobContainerPublicAccessType accessType : accessTypes)
        {
            String name = UUID.randomUUID().toString();
            CloudBlobContainer container = bClient.getContainerReference(name);
            assertNull(container.properties.getPublicAccess());
            container.create(accessType, null, null);
            assertEquals(accessType, container.properties.getPublicAccess());

            CloudBlobContainer containerRef = bClient.getContainerReference(name);
            assertNull(containerRef.properties.getPublicAccess());
            BlobContainerPermissions permissions = containerRef.downloadPermissions();
            assertEquals(accessType, containerRef.properties.getPublicAccess());
            assertEquals(accessType, permissions.getPublicAccess());
            
            CloudBlobContainer containerRef2 = bClient.getContainerReference(name);
            assertEquals(null, containerRef2.properties.getPublicAccess());
            containerRef2.exists();
            assertEquals(accessType, containerRef2.properties.getPublicAccess());

            String name2 = UUID.randomUUID().toString();
            CloudBlobContainer container2 = bClient.getContainerReference(name2);
            assertNull(container2.properties.getPublicAccess());
            container2.create();
            assertEquals(BlobContainerPublicAccessType.OFF, container2.properties.getPublicAccess());

            BlobContainerPermissions permissions2 = new BlobContainerPermissions();
            permissions2.setPublicAccess(accessType);
            container2.uploadPermissions(permissions);
            assertEquals(accessType, container2.properties.getPublicAccess());
            
            CloudBlobContainer containerRef3 = bClient.getContainerReference(name);
            containerRef3.downloadAttributes();
            assertEquals(accessType, containerRef3.properties.getPublicAccess());

            String name3 = UUID.randomUUID().toString();
            CloudBlobContainer container3 = bClient.getContainerReference(name3);
            container3.create(null, null, null);
            assertEquals(BlobContainerPublicAccessType.OFF, container3.properties.getPublicAccess());

            container.delete();
            container2.delete();
            container3.delete();
        }
    }

    /**
     * List containers and fetch attributes with public access
     *
     * @throws StorageException
     * @throws URISyntaxException
     */
    @Test
    @Category({ DevFabricTests.class, DevStoreTests.class, CloudTests.class })
    public void testListContainersAndFetchAttributesWithPublicAccess() throws StorageException, URISyntaxException {
        CloudBlobClient bClient = BlobTestHelper.createCloudBlobClient();
        String name = UUID.randomUUID().toString();
        CloudBlobContainer container = bClient.getContainerReference(name);
        container.create();
        BlobContainerPublicAccessType[] accessTypes = {BlobContainerPublicAccessType.BLOB,
                BlobContainerPublicAccessType.CONTAINER, BlobContainerPublicAccessType.OFF};
        BlobContainerPermissions permissions = new BlobContainerPermissions();
        for (BlobContainerPublicAccessType accessType : accessTypes) {
            permissions.setPublicAccess(accessType);
            container.uploadPermissions(permissions);
            assertEquals(accessType, container.properties.getPublicAccess());

            CloudBlobContainer container2 = bClient.getContainerReference(name);
            assertNull(container2.properties.getPublicAccess());
            container2.downloadAttributes();
            assertEquals(accessType, container2.properties.getPublicAccess());
            
            CloudBlobContainer container3 = bClient.getContainerReference(name);
            assertNull(container3.properties.getPublicAccess());
            assertEquals(accessType, container3.downloadPermissions().getPublicAccess());

            Iterator<CloudBlobContainer> results = bClient.listContainers(name, ContainerListingDetails.NONE, null, null).iterator();
            assertTrue(results.hasNext());
            assertEquals(accessType, results.next().properties.getPublicAccess());
            assertFalse(results.hasNext());
        }

        container.delete();
    }

    @Test
    @Category({ CloudTests.class })
    public void testGetServiceStats() throws StorageException {
        CloudBlobClient bClient = BlobTestHelper.createCloudBlobClient();
        bClient.getDefaultRequestOptions().setLocationMode(LocationMode.SECONDARY_ONLY);
        BlobTestHelper.verifyServiceStats(bClient.getServiceStats());
    }

    @Test
    @Category({ CloudTests.class})
    public void testGetAccountInformation() throws StorageException, URISyntaxException, InvalidKeyException {
        // Shared key
        CloudBlobClient bClient = BlobTestHelper.createCloudBlobClient();
        bClient.getDefaultRequestOptions().setLocationMode(LocationMode.PRIMARY_ONLY);

        AccountInformation accountInformation = bClient.downloadAccountInfo();
        assertNotNull(accountInformation.getAccountKind());
        assertNotNull(accountInformation.getSkuName());

        // SAS
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.setTime(new Date());
        cal.add(Calendar.SECOND, 60);
        SharedAccessAccountPolicy policy = new SharedAccessAccountPolicy();
        policy.setSharedAccessExpiryTime(cal.getTime());
        policy.setPermissionsFromString("r");
        policy.setResourceTypes(EnumSet.of(SharedAccessAccountResourceType.SERVICE));
        policy.setServiceFromString("b");
        CloudBlobClient sasClient = TestHelper.createCloudBlobClient(policy, false);

        accountInformation = sasClient.downloadAccountInfo();
        assertNotNull(accountInformation.getAccountKind());
        assertNotNull(accountInformation.getSkuName());
    }

    @Test
    @Category({ DevFabricTests.class, DevStoreTests.class, CloudTests.class })
    public void testSingleBlobPutThresholdInBytes() throws URISyntaxException, StorageException, IOException {
        CloudBlobClient bClient = BlobTestHelper.createCloudBlobClient();

        try {
            bClient.getDefaultRequestOptions().setSingleBlobPutThresholdInBytes(
                    BlobConstants.MAX_SINGLE_UPLOAD_BLOB_SIZE_IN_BYTES + 1);
            fail("Cannot set upload blob threshold above 256 MB");
        }
        catch (IllegalArgumentException e) {
            assertEquals(
                    "The argument is out of range. Argument name: singleBlobPutThresholdInBytes, Value passed: 268435457.",
                    e.getMessage());
        }

        try {
            bClient.getDefaultRequestOptions().setSingleBlobPutThresholdInBytes(Constants.MB - 1);
            fail("Cannot set upload blob threshold below 1 MB");
        }
        catch (IllegalArgumentException e) {
            assertEquals(
                    "The argument is out of range. Argument name: singleBlobPutThresholdInBytes, Value passed: 1048575.",
                    e.getMessage());
        }

        int maxSize = 2 * Constants.MB;

        bClient.getDefaultRequestOptions().setSingleBlobPutThresholdInBytes(maxSize);

        final ArrayList<Boolean> callList = new ArrayList<Boolean>();
        OperationContext sendingRequestEventContext = new OperationContext();
        sendingRequestEventContext.getSendingRequestEventHandler().addListener(new StorageEvent<SendingRequestEvent>() {

            @Override
            public void eventOccurred(SendingRequestEvent eventArg) {
                assertEquals(eventArg.getRequestResult(), eventArg.getOpContext().getLastResult());
                callList.add(true);
            }
        });

        assertEquals(0, callList.size());

        CloudBlobContainer container = null;
        try {
            container = bClient.getContainerReference(BlobTestHelper.generateRandomContainerName());
            container.createIfNotExists();
            CloudBlockBlob blob = container.getBlockBlobReference(BlobTestHelper
                    .generateRandomBlobNameWithPrefix("uploadThreshold"));

            // this should make a single call as it is less than the max
            blob.upload(BlobTestHelper.getRandomDataStream(maxSize - 1), maxSize - 1, null, null,
                    sendingRequestEventContext);

            assertEquals(1, callList.size());

            // this should make one call as it is equal to the max
            blob.upload(BlobTestHelper.getRandomDataStream(maxSize), maxSize, null, null, sendingRequestEventContext);

            assertEquals(2, callList.size());

            // this should make two calls as it is greater than the max
            blob.upload(BlobTestHelper.getRandomDataStream(maxSize + 1), maxSize + 1, null, null,
                    sendingRequestEventContext);

            assertEquals(4, callList.size());
        }
        finally {
            container.deleteIfExists();
        }
    }

    @Test
    @Category({ DevFabricTests.class, DevStoreTests.class, CloudTests.class })
    public void testUploadBlobFromFileSinglePut() throws URISyntaxException, StorageException, IOException {
        CloudBlobClient bClient = BlobTestHelper.createCloudBlobClient();

        final ArrayList<Boolean> callList = new ArrayList<Boolean>();
        OperationContext sendingRequestEventContext = new OperationContext();
        sendingRequestEventContext.getSendingRequestEventHandler().addListener(new StorageEvent<SendingRequestEvent>() {

            @Override
            public void eventOccurred(SendingRequestEvent eventArg) {
                assertEquals(eventArg.getRequestResult(), eventArg.getOpContext().getLastResult());
                callList.add(true);
            }
        });

        assertEquals(0, callList.size());

        CloudBlobContainer container = null;
        File sourceFile = File.createTempFile("sourceFile", ".tmp");
        try {
            container = bClient.getContainerReference(BlobTestHelper.generateRandomContainerName());
            container.createIfNotExists();
            CloudBlockBlob blob = container.getBlockBlobReference(BlobTestHelper
                    .generateRandomBlobNameWithPrefix("uploadThreshold"));

            sourceFile = File.createTempFile("sourceFile", ".tmp");

            int fileSize = 10 * 1024;
            byte[] buffer = BlobTestHelper.getRandomBuffer(fileSize);
            FileOutputStream fos = new FileOutputStream(sourceFile);
            fos.write(buffer);
            fos.close();

            // This should make a single call even though FileInputStream is not seekable because of the optimizations
            // from wrapping it in a MarkableFileInputStream
            blob.upload(new FileInputStream(sourceFile), fileSize - 1, null, null, sendingRequestEventContext);

            assertEquals(1, callList.size());
        }
        finally {
            container.deleteIfExists();

            if (sourceFile.exists()) {
                sourceFile.delete();
            }
        }
    }
}