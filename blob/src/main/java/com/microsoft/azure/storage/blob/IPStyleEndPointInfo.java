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
package com.microsoft.azure.storage.blob;

/**
 * IPEndpointStyleInfo is used for IP endpoint style URL. It's commonly used when
 * working with Azure storage emulator or testing environments. For Example:
 * "https://10.132.141.33/accountname/container/blob"
 */
public final class IPStyleEndPointInfo {

    private String accountName;

    private Integer port;

    /**
     * The account name. For Example: "https://10.132.41.33/accountname"
     */
    public String accountName() {
        return accountName;
    }

    /**
     * The account name. For Example: "https://10.132.41.33/accountname"
     * Note: if the standard <account>.blob.core.windows.net is presented to {@link BlobURLParts},
     * {@link IPStyleEndPointInfo} will be null because the url may be a custom domain. In these cases, if the
     * account happens to be present in the host, it will simply remain as a part of the host field in the
     * {@link BlobURLParts} object.
     * {@link IPStyleEndPointInfo} is present and populated with accountname only if an IP address is used to present
     * the blob url
     */
    public IPStyleEndPointInfo withAccountName(String accountName) {
        this.accountName = accountName;
        return this;
    }

    /**
     * The port number of the IP address. For Example: "https://10.132.41.33:80/accountname"
     */
    public Integer port() {
        return port;
    }

    /**
     * The port number of the IP address. For Example: "https://10.132.41.33:80/accountname"
     */
    public IPStyleEndPointInfo withPort(Integer port) {
        this.port = port;
        return this;
    }
}
