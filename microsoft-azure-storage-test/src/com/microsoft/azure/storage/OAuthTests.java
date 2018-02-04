package com.microsoft.azure.storage;

import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.naming.ServiceUnavailableException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * OAuth tests.
 */
public class OAuthTests {
    /**
     * Test OAuth with respect to blob client
     */
    @Test
    @Category({ TestRunners.DevFabricTests.class, TestRunners.DevStoreTests.class, TestRunners.CloudTests.class })
    public void testOAuthTokenWithBlobClient() throws StorageException, URISyntaxException, MalformedURLException, InterruptedException, ExecutionException, ServiceUnavailableException {
        // get the standard test account
        CloudStorageAccount account = TestHelper.getAccount();

        // create a token credential and replace the account's credential
        StorageCredentialsToken storageCredentialsToken = new StorageCredentialsToken(TestHelper.getAccountName(), TestHelper.generateOAuthToken());
        account.setCredentials(storageCredentialsToken);

        // test to make sure authentication is working
        CloudBlobClient blobClient = account.createCloudBlobClient();
        blobClient.downloadServiceProperties();

        // change the token to see it fail
        try {
            storageCredentialsToken.updateToken("BLA");
            blobClient.downloadServiceProperties();
            fail();
        } catch (StorageException e) {
            assertEquals("AuthenticationFailed", e.getErrorCode());
        }

        // change the token again to see it succeed
        storageCredentialsToken.updateToken(TestHelper.generateOAuthToken());
        blobClient.downloadServiceProperties();
    }

    /**
     * Test OAuth with respect to queue client
     */
    @Test
    @Category({ TestRunners.DevFabricTests.class, TestRunners.DevStoreTests.class, TestRunners.CloudTests.class })
    public void testOAuthTokenWithQueueClient() throws StorageException, URISyntaxException, MalformedURLException, InterruptedException, ExecutionException, ServiceUnavailableException {
        // get the standard test account
        CloudStorageAccount account = TestHelper.getAccount();

        // create a token credential and replace the account's credential
        StorageCredentialsToken storageCredentialsToken = new StorageCredentialsToken(TestHelper.getAccountName(), TestHelper.generateOAuthToken());
        account.setCredentials(storageCredentialsToken);

        // test to make sure authentication is working
        CloudQueueClient queueClient = account.createCloudQueueClient();
        queueClient.downloadServiceProperties();

        // change the token to see it fail
        try {
            storageCredentialsToken.updateToken("BLA");
            queueClient.downloadServiceProperties();
            fail();
        } catch (StorageException e) {
            assertEquals("AuthenticationFailed", e.getErrorCode());
        }

        // change the token again to see it succeed
        storageCredentialsToken.updateToken(TestHelper.generateOAuthToken());
        queueClient.downloadServiceProperties();
    }
}
