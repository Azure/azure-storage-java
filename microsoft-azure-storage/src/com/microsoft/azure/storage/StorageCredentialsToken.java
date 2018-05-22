package com.microsoft.azure.storage;

import java.net.URI;

/**
 * Represents storage account credentials, based on an authentication token, for accessing the Microsoft Azure
 * storage services.
 */
public final class StorageCredentialsToken extends StorageCredentials{
    /**
     * Stores the token for the credentials.
     */
    private volatile String token;

    /**
     * Stores the account name.
     */
    private volatile String accountName;

    /**
     * Creates an instance of the <code>StorageCredentialsToken</code> class, using the specified token.
     * Token credentials must only be used with HTTPS requests on the blob and queue services.
     * The specified token is stored as a <code>String</code>.
     *
     * @param token
     *            A <code>String</code> that represents the token.
     */
    public StorageCredentialsToken(String accountName, String token) {
        this.accountName = accountName;
        this.token = token;
    }

    /**
     * Gets whether this <code>StorageCredentials</code> object only allows access via HTTPS.
     *
     * @return A <code>boolean</code> representing whether this <code>StorageCredentials</code>
     *         object only allows access via HTTPS.
     */
    @Override
    public boolean isHttpsOnly() {
        return true;
    }

    /**
     * Gets the token.
     *
     * @return A <code>String</code> that contains the token.
     */
    public String getToken() {
        return this.token;
    }

    /**
     * Sets the token to be used when authenticating HTTPS requests.
     *
     * @param token
     *        A <code>String</code> that represents the access token to be used when authenticating HTTPS requests.
     */
    public synchronized void updateToken(final String token) {
        this.token = token;
    }

    /**
     * Gets the account name.
     *
     * @return A <code>String</code> that contains the account name.
     */
    @Override
    public String getAccountName() {
        return this.accountName;
    }

    /**
     * Returns a <code>String</code> that represents this instance, optionally including sensitive data.
     *
     * @param exportSecrets
     *            <code>true</code> to include sensitive data in the return string; otherwise, <code>false</code>.
     *
     * @return A <code>String</code> that represents this object, optionally including sensitive data.
     */
    @Override
    public String toString(final boolean exportSecrets) {
        return String.format("%s=%s", CloudStorageAccount.ACCOUNT_TOKEN_NAME, exportSecrets ? this.token
                        : "[token hidden]");
    }

    @Override
    public URI transformUri(URI resourceUri, OperationContext opContext) {
        return resourceUri;
    }

    @Override
    public StorageUri transformUri(StorageUri resourceUri, OperationContext opContext) {
        return resourceUri;
    }
}
