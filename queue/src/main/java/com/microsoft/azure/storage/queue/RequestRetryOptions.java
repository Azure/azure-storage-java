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

import java.util.concurrent.TimeUnit;

/**
 * Options for configuring the {@link RequestRetryFactory}. Please refer to the Factory for more information. Note
 * that there is no option for overall operation timeout. This is because Rx object have a timeout field which provides
 * this functionality.
 */
public final class RequestRetryOptions {

    /**
     * An object representing default retry values: Exponential backoff, maxTries=4, tryTimeout=30, retryDelayInMs=4000,
     * maxRetryDelayInMs=120000, secondaryHost=null.
     */
    public static final RequestRetryOptions DEFAULT = new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, null,
            null, null, null);

    /**
     * A {@link RetryPolicyType} telling the pipeline what kind of retry policy to use.
     */
    private RetryPolicyType retryPolicyType = RetryPolicyType.EXPONENTIAL;

    final private int maxTries;

    final private int tryTimeout;

    final private long retryDelayInMs;

    final private long maxRetryDelayInMs;

    /**
     * Configures how the {@link com.microsoft.rest.v2.http.HttpPipeline} should retry requests.
     *
     * @param retryPolicyType
     *         A {@link RetryPolicyType} specifying the type of retry pattern to use. A value of {@code null} accepts the
     *         default.
     * @param maxTries
     *         Specifies the maximum number of attempts an operation will be tried before producing an error. A value of
     *         {@code null} means that you accept our default policy. A value of 1 means 1 try and no retries.
     * @param tryTimeout
     *         Indicates the maximum time allowed for any single try of an HTTP request. A value of {@code null} means that
     *         you accept our default. NOTE: When transferring large amounts of data, the default TryTimeout will probably
     *         not be sufficient. You should override this value based on the bandwidth available to the host machine and
     *         proximity to the Storage service. A good starting point may be something like (60 seconds per MB of
     *         anticipated-payload-size).
     * @param retryDelayInMs
     *         Specifies the amount of delay to use before retrying an operation. A value of {@code null} means you accept
     *         the default value. The delay increases (exponentially or linearly) with each retry up to a maximum specified
     *         by MaxRetryDelay. If you specify {@code null}, then you must also specify {@code null} for MaxRetryDelay.
     * @param maxRetryDelayInMs
     *         Specifies the maximum delay allowed before retrying an operation. A value of {@code null} means you accept
     *         the default value. If you specify {@code null}, then you must also specify {@code null} for RetryDelay.
     */
    public RequestRetryOptions(RetryPolicyType retryPolicyType, Integer maxTries, Integer tryTimeout,
            Long retryDelayInMs, Long maxRetryDelayInMs) {
        this.retryPolicyType = retryPolicyType == null ? RetryPolicyType.EXPONENTIAL : retryPolicyType;
        if (maxTries != null) {
            Utility.assertInBounds("maxRetries", maxTries, 1, Integer.MAX_VALUE);
            this.maxTries = maxTries;
        } else {
            this.maxTries = 4;
        }

        if (tryTimeout != null) {
            Utility.assertInBounds("tryTimeoutInMs", tryTimeout, 1, Long.MAX_VALUE);
            this.tryTimeout = tryTimeout;
        } else {
            this.tryTimeout = 60;
        }

        if ((retryDelayInMs == null && maxRetryDelayInMs != null) ||
                (retryDelayInMs != null && maxRetryDelayInMs == null)) {
            throw new IllegalArgumentException("Both retryDelay and maxRetryDelay must be null or neither can be null");
        }

        if (retryDelayInMs != null && maxRetryDelayInMs != null) {
            Utility.assertInBounds("maxRetryDelayInMs", maxRetryDelayInMs, 1, Long.MAX_VALUE);
            Utility.assertInBounds("retryDelayInMs", retryDelayInMs, 1, maxRetryDelayInMs);
            this.maxRetryDelayInMs = maxRetryDelayInMs;
            this.retryDelayInMs = retryDelayInMs;
        } else {
            switch (this.retryPolicyType) {
                case EXPONENTIAL:
                    this.retryDelayInMs = TimeUnit.SECONDS.toMillis(4);
                    break;
                case FIXED:
                    this.retryDelayInMs = TimeUnit.SECONDS.toMillis(30);
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognize retry policy type.");
            }
            this.maxRetryDelayInMs = TimeUnit.SECONDS.toMillis(120);
        }
    }

    int maxTries() {
        return this.maxTries;
    }

    int tryTimeout() {
        return this.tryTimeout;
    }


    /*
    Azure Queue Storage Service does not provide support for secondary host. It returns null
    when retry policy ask the secondary host and so secondary host is never tried.
    */
    String secondaryHost() {
        return null;
    }

    long retryDelayInMs() {
        return retryDelayInMs;
    }

    long maxRetryDelayInMs() {
        return maxRetryDelayInMs;
    }

    /**
     * Calculates how long to delay before sending the next request.
     *
     * @param tryCount
     *         An {@code int} indicating which try we are on.
     *
     * @return A {@code long} value of how many milliseconds to delay.
     */
    long calculateDelayInMs(int tryCount) {
        long delay = 0;
        switch (this.retryPolicyType) {
            case EXPONENTIAL:
                delay = (pow(2L, tryCount - 1) - 1L) * this.retryDelayInMs;
                break;

            case FIXED:
                delay = this.retryDelayInMs;
                break;
        }

        return Math.min(delay, this.maxRetryDelayInMs);
    }

    private long pow(long number, int exponent) {
        long result = 1;
        for (int i = 0; i < exponent; i++) {
            result *= number;
        }

        return result;
    }
}
