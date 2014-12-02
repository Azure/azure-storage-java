package com.microsoft.azure.storage.http;

import com.microsoft.azure.storage.http.AzureHttpURLConnection;
import sun.net.www.protocol.http.Handler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;

public class AzureHttpHandler extends Handler {
    private final InetAddress localAddress;

    public AzureHttpHandler(InetAddress localAddress) {
        this.localAddress = localAddress;
    }

    @Override
    protected URLConnection openConnection(URL u) throws IOException {
        return openConnection(u, null);
    }

    @Override
    protected URLConnection openConnection(URL u, Proxy p) throws IOException {
        return new AzureHttpURLConnection(u, p, localAddress, this);
    }
}
