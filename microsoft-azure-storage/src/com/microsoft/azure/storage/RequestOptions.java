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
/**
 * 
 */
package com.microsoft.azure.storage;

import java.util.Date;

import com.microsoft.azure.storage.core.Utility;

/**
 * Represents the options to use while processing a given request.
 */
public abstract class RequestOptions {

    /**
     * The instance of the {@link RetryPolicyFactory} interface to use for the request.
     */
    private RetryPolicyFactory retryPolicyFactory;

    /**
     * The timeout interval, in milliseconds, to use for the request.
     */
    private Integer timeoutIntervalInMs;

    /**
     * The location mode of the request.
     */
    private LocationMode locationMode;

    /**
     * The maximum execution time, in milliseconds, across all potential retries.
     */
    private Integer maximumExecutionTimeInMs;

    /**
     * Represents the start time, in milliseconds, from the client's perspective.
     */
    private Long operationExpiryTime;
    
    /**
     * A value to indicate whether all data written and read must be encrypted.
     */
    private Boolean requireEncryption;

    /**
     * A value to indicate whether we should disable socket keep-alive.
     */
    private Boolean disableHttpsSocketKeepAlive;

    /**
     * Creates an instance of the <code>RequestOptions</code> class.
     */
    public RequestOptions() {
        // Empty Default Ctor
    }

    /**
     * Creates an instance of the <code>RequestOptions</code> class by copying values from another
     * <code>RequestOptions</code> instance.
     * 
     * @param other
     *            A <code>RequestOptions</code> object that represents the request options to copy.
     */
    public RequestOptions(final RequestOptions other) {
        if (other != null) {
            this.setRetryPolicyFactory(other.getRetryPolicyFactory());
            this.setTimeoutIntervalInMs(other.getTimeoutIntervalInMs());
            this.setLocationMode(other.getLocationMode());
            this.setMaximumExecutionTimeInMs(other.getMaximumExecutionTimeInMs());
            this.setOperationExpiryTimeInMs(other.getOperationExpiryTimeInMs());
            this.setRequireEncryption(other.requireEncryption());
            this.setDisableHttpsSocketKeepAlive(other.disableHttpsSocketKeepAlive());
        }
    }

    /**
     * Populates the default timeout, retry policy, and location mode from client if they are null.
     * 
     * @param modifiedOptions
     *            The input options to copy from when applying defaults
     */
    protected static void applyBaseDefaultsInternal(final RequestOptions modifiedOptions) {
        Utility.assertNotNull("modifiedOptions", modifiedOptions);
        if (modifiedOptions.getRetryPolicyFactory() == null) {
            modifiedOptions.setRetryPolicyFactory(new RetryExponentialRetry());
        }

        if (modifiedOptions.getLocationMode() == null) {
            modifiedOptions.setLocationMode(LocationMode.PRIMARY_ONLY);
        }
        
        if (modifiedOptions.requireEncryption() == null) {
            modifiedOptions.setRequireEncryption(false);
        }

        if (modifiedOptions.disableHttpsSocketKeepAlive() == null) {
            modifiedOptions.setDisableHttpsSocketKeepAlive(false);
        }
    }

    /**
     * Populates any null fields in the first requestOptions object with values from the second requestOptions object.
     */
    protected static void populateRequestOptions(RequestOptions modifiedOptions,
            final RequestOptions clientOptions, final boolean setStartTime) {
        if (modifiedOptions.getRetryPolicyFactory() == null) {
            modifiedOptions.setRetryPolicyFactory(clientOptions.getRetryPolicyFactory());
        }

        if (modifiedOptions.getLocationMode() == null) {
            modifiedOptions.setLocationMode(clientOptions.getLocationMode());
        }

        if (modifiedOptions.getTimeoutIntervalInMs() == null) {
            modifiedOptions.setTimeoutIntervalInMs(clientOptions.getTimeoutIntervalInMs());
        }
        
        if (modifiedOptions.requireEncryption() == null) {
            modifiedOptions.setRequireEncryption(clientOptions.requireEncryption());
        }

        if (modifiedOptions.getMaximumExecutionTimeInMs() == null) {
            modifiedOptions.setMaximumExecutionTimeInMs(clientOptions.getMaximumExecutionTimeInMs());
        }

        if (modifiedOptions.getMaximumExecutionTimeInMs() != null
                && modifiedOptions.getOperationExpiryTimeInMs() == null && setStartTime) {
            modifiedOptions.setOperationExpiryTimeInMs(new Date().getTime()
                    + modifiedOptions.getMaximumExecutionTimeInMs());
        }

        if (modifiedOptions.disableHttpsSocketKeepAlive() == null) {
            modifiedOptions.setDisableHttpsSocketKeepAlive(clientOptions.disableHttpsSocketKeepAlive());
        }
    }

    /**
     * Gets the retry policy to use for this request. For more information about the retry policy defaults, see
     * {@link #setRetryPolicyFactory(RetryPolicyFactory)}.
     * 
     * @return An {@link RetryPolicyFactory} object that represents the current retry policy.
     * 
     * @see RetryPolicy
     * @see RetryExponentialRetry
     * @see RetryLinearRetry
     * @see RetryNoRetry
     */
    public final RetryPolicyFactory getRetryPolicyFactory() {
        return this.retryPolicyFactory;
    }

    /**
     * Returns the timeout value for this request. For more information about the timeout defaults, see
     * {@link #setTimeoutIntervalInMs(Integer)}.
     * 
     * @return The current timeout value, in milliseconds, for this request.
     */
    public final Integer getTimeoutIntervalInMs() {
        return this.timeoutIntervalInMs;
    }

    /**
     * Gets the default location mode for this request. For more information about location mode, see
     * {@link #setLocationMode(LocationMode)}.
     * 
     * @return A {@link LocationMode} object that represents the location mode for this request.
     */
    public final LocationMode getLocationMode() {
        return this.locationMode;
    }

    /**
     * Gets the maximum execution time for this request. For more information about maximum execution time defaults, see
     * {@link #setMaximumExecutionTimeInMs(Integer)}.
     * 
     * @return The current maximum execution time, in milliseconds, for this request.
     */
    public Integer getMaximumExecutionTimeInMs() {
        return this.maximumExecutionTimeInMs;
    }

    /**
     * Gets a value to indicate whether all data written and read must be encrypted. Use <code>true</code> to
     * encrypt/decrypt data for transactions; otherwise, <code>false</code>. For more
     * information about require encryption defaults, see {@link #setRequireEncryption(Boolean)}.
     *
     * @return A value to indicate whether all data written and read must be encrypted.
     */
    public Boolean requireEncryption() {
        return this.requireEncryption;
    }

    /**
     * Gets a value to indicate whether https socket keep-alive should be disabled. Use <code>true</code> to disable
     * keep-alive; otherwise, <code>false</code>. For more information about disableHttpsSocketKeepAlive defaults, see
     * {@link ServiceClient#getDefaultRequestOptions()}
     *
     * @return A value to indicate whether https socket keep-alive should be disabled.
     */
    public Boolean disableHttpsSocketKeepAlive() {
        return this.disableHttpsSocketKeepAlive;
    }

    /**
     * RESERVED FOR INTERNAL USE.
     * 
     * Returns the time at which this operation expires. This is computed by adding the time the operation begins and
     * the maximum execution time and will be null if maximum execution time is null. For more information about maximum
     * execution time, see {@link #setMaximumExecutionTimeInMs(Integer)}.
     * 
     * @return The current operation expiry time, in milliseconds, for this request.
     */
    public Long getOperationExpiryTimeInMs() {
        return this.operationExpiryTime;
    }

    /**
     * Sets the RetryPolicyFactory object to use for this request.
     * <p>
     * The default RetryPolicyFactory is set in the client and is by default {@link RetryExponentialRetry}. You can
     * change the RetryPolicyFactory on this request by setting this property. You can also change the value on the
     * {@link ServiceClient#getDefaultRequestOptions()} object so that all subsequent requests made via the service
     * client will use that RetryPolicyFactory.
     * 
     * @param retryPolicyFactory
     *            the RetryPolicyFactory object to use when making service requests.
     * 
     * @see RetryPolicy
     * @see RetryExponentialRetry
     * @see RetryLinearRetry
     * @see RetryNoRetry
     */
    public final void setRetryPolicyFactory(final RetryPolicyFactory retryPolicyFactory) {
        this.retryPolicyFactory = retryPolicyFactory;
    }

    /**
     * Sets the timeout to use when making this request.
     * <p>
     * The server timeout interval begins at the time that the complete request has been received by the service, and
     * the server begins processing the response. If the timeout interval elapses before the response is returned to the
     * client, the operation times out. The timeout interval resets with each retry, if the request is retried.
     * <p>
     * The default server timeout is set in the client and is by default null, indicating no server timeout. You can
     * change the server timeout on this request by setting this property. You can also change the value on the
     * {@link ServiceClient#getDefaultRequestOptions()} object so that all subsequent requests made via the service
     * client will use that server timeout.
     * 
     * @param timeoutIntervalInMs
     *            The timeout, in milliseconds, to use for this request.
     */
    public final void setTimeoutIntervalInMs(final Integer timeoutIntervalInMs) {
        this.timeoutIntervalInMs = timeoutIntervalInMs;
    }

    /**
     * Sets the {@link LocationMode} for this request.
     * <p>
     * The default {@link LocationMode} is set in the client and is by default {@link LocationMode#PRIMARY_ONLY}. You
     * can change the {@link LocationMode} on this request by setting this property. You can also change the value on
     * the {@link ServiceClient#getDefaultRequestOptions()} object so that all subsequent requests made via the service
     * client will use that {@link LocationMode}.
     * 
     * @param locationMode
     *            the locationMode to set
     */
    public void setLocationMode(final LocationMode locationMode) {
        this.locationMode = locationMode;
    }

    /**
     * Sets the maximum execution time to use when making this request.
     * <p>
     * The maximum execution time interval begins at the time that the client begins building the request. The maximum
     * execution time is checked intermittently while uploading data, downloading data, and before executing retries.
     * The service will continue to upload, download, and retry until the maximum execution time is reached. At that
     * time, any partial uploads or downloads will be cancelled and an exception will be thrown.
     * <p>
     * The default maximum execution is set in the client and is by default null, indicating no maximum time. You can
     * change the maximum execution time on this request by setting this property. You can also change the value on the
     * {@link ServiceClient#getDefaultRequestOptions()} object so that all subsequent requests made via the service
     * client will use the maximum execution time.
     * 
     * @param maximumExecutionTimeInMs
     *            The maximum execution time, in milliseconds, to use for this request.
     */
    public void setMaximumExecutionTimeInMs(Integer maximumExecutionTimeInMs) {
        this.maximumExecutionTimeInMs = maximumExecutionTimeInMs;
    }
    
    /**
     * Sets a value to indicate whether all data written and read must be encrypted. Use <code>true</code> to
     * encrypt/decrypt data for transactions; otherwise, <code>false</code>.
     * <p>
     * The default is set in the client and is by default false, indicating encryption is not required. You can change
     * the value on this request by setting this property. You can also change the value on the
     * {@link ServiceClient#getDefaultRequestOptions()} object so that all subsequent requests made via the
     * service client will use the appropriate value.
     * 
     * @param requireEncryption
     *            A value to indicate whether all data written and read must be encrypted.
     */
    public void setRequireEncryption(Boolean requireEncryption) {
        this.requireEncryption = requireEncryption;
    }

    /**
     * Sets a value to indicate whether https socket keep-alive should be disabled. Use <code>true</code> to disable
     * keep-alive; otherwise, <code>false</code>
     * <p>
     * The default is set in the client and is by default false, indicating that https socket keep-alive will be
     * enabled. You can change the value on this request by setting this property. You can also change the value on
     * on the {@link ServiceClient#getDefaultRequestOptions()} object so that all subsequent requests made via the
     * service client will use the appropriate value.
     * <p>
     * Setting keep-alive on https sockets is to work around a bug in the JVM where connection timeouts are not honored
     * on retried requests. In those cases, we use socket keep-alive as a fallback. Unfortunately, the timeout value
     * must be taken from a JVM property rather than configured locally. Therefore, in rare cases the JVM has configured
     * aggressively short keep-alive times, it may be beneficial to disable the use of keep-alives lest they interfere
     * with long running data transfer operations.
     *
     * @param disableHttpsSocketKeepAlive
     *           A value to indicate whether https socket keep-alive should be disabled.
     */
    public void setDisableHttpsSocketKeepAlive(Boolean disableHttpsSocketKeepAlive) {
        this.disableHttpsSocketKeepAlive = disableHttpsSocketKeepAlive;
    }

    /**
     * RESERVED FOR INTERNAL USE.
     * 
     * Returns the time at which this operation expires. This is computed by adding the time the operation begins and
     * the maximum execution time and will be null if maximum execution time is null. For more information about maximum
     * execution time, see {@link #setMaximumExecutionTimeInMs(Integer)}.
     * 
     * @param operationExpiryTime
     *            the operationExpiryTime to set
     */
    private void setOperationExpiryTimeInMs(final Long operationExpiryTime) {
        this.operationExpiryTime = operationExpiryTime;
    }
}
