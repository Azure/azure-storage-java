package com.microsoft.azure.storage.http;

import sun.net.www.http.HttpClient;
import sun.net.www.protocol.http.Handler;
import sun.net.www.protocol.http.HttpURLConnection;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Proxy;
import java.net.URL;

public class AzureHttpURLConnection extends HttpURLConnection {
    private final InetAddress localAddress;

    AzureHttpURLConnection(URL u, Proxy p, InetAddress localAddress, Handler handler) {
        super(u, p, handler);
        this.localAddress = localAddress;
    }

    @Override
    protected void setNewClient(URL url, boolean useCache) throws IOException {
        proxiedConnect(url, null, -1, useCache);
    }

    @Override
    protected void proxiedConnect(URL url, String proxyHost, int proxyPort, boolean useCache) throws IOException {
        int connectTimeout = http.getConnectTimeout();
        int readTimeout = http.getReadTimeout();
        http = AzureHttpClient.New(url, proxyHost, proxyPort, localAddress, useCache, connectTimeout);
        http.setReadTimeout(readTimeout);
    }

    @Override
    protected HttpClient getNewHttpClient(URL url, Proxy p, int connectTimeout) throws IOException {
        return AzureHttpClient.New(url, p, localAddress, connectTimeout);
    }

    @Override
    protected HttpClient getNewHttpClient(URL url, Proxy p, int connectTimeout, boolean useCache) throws IOException {
        return AzureHttpClient.New(url, p, localAddress, connectTimeout, useCache);
    }
}
