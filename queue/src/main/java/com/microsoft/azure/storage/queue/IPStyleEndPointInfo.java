package com.microsoft.azure.storage.queue;

/**
 * IPEndpointStyleInfo is used for IP endpoint style URL. It's commonly used when
 * working with Azure storage emulator or testing environments. For Example:
 * "https://10.132.141.33/accountname/queuename"
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
