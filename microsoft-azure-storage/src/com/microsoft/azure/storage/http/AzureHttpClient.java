package com.microsoft.azure.storage.http;

import sun.net.www.http.HttpClient;
import sun.net.www.protocol.http.HttpURLConnection;
import sun.util.logging.PlatformLogger;

import java.io.IOException;
import java.net.*;

public class AzureHttpClient extends HttpClient {
    private boolean inCacheOverride;
    private final InetAddress localAddress;

    private AzureHttpClient(URL url, Proxy p, InetAddress localAddress, int to) throws IOException {
        this.localAddress = localAddress;

        proxy = (p == null) ? Proxy.NO_PROXY : p;
        this.host = url.getHost();
        this.url = url;
        port = url.getPort();
        if (port == -1) {
            port = getDefaultPort();
        }
        setConnectTimeout(to);

        openServer();
    }

    public static HttpClient New(URL url, Proxy p, InetAddress localAddress, int to, boolean useCache) throws IOException {
        if (p == null) {
            p = Proxy.NO_PROXY;
        }

        AzureHttpClient ret = null;
        /* see if one's already around */
        if (useCache) {
            HttpClient cachedClient = kac.get(url, null);
            if (cachedClient != null) {
                if (cachedClient instanceof AzureHttpClient) {
                    ret = (AzureHttpClient)cachedClient;
                    if ((ret.localAddress != null && ret.localAddress.equals(localAddress))
                            || (ret.localAddress == null && localAddress == null)
                            || (ret.proxy != null && ret.proxy.equals(p))
                            || (ret.proxy == null && p == null)) {
                        synchronized (ret) {
                            ret.cachedHttpClient = true;
                            assert ret.inCacheOverride;
                            ret.inCacheOverride = false;
                            PlatformLogger logger = HttpURLConnection.getHttpLogger();
                            if (logger.isLoggable(PlatformLogger.FINEST)) {
                                logger.finest("KeepAlive stream retrieved from the cache, " + ret);
                            }
                        }
                    } else {
                        // We cannot return this connection to the cache as it's
                        // KeepAliveTimeout will get reset. We simply close the connection.
                        // This should be fine as it is very rare that a connection
                        // to the same host will not use the same proxy.
                        synchronized (ret) {
                            ret.inCacheOverride = false;
                            ret.closeServer();
                        }
                        ret = null;
                    }
                } else {
                    // We would hope that this won't happen - outgoing connections to Azure URLs should
                    // always go through this class. If for some reason it does happen, try to close
                    // the connection and make a new one.
                    synchronized (cachedClient) {
                        cachedClient.closeServer();
                    }
                    ret = null;
                }
            }
        }
        if (ret == null) {
            ret = new AzureHttpClient(url, p, localAddress, to);
        } else {
            SecurityManager security = System.getSecurityManager();
            if (security != null) {
                    if (ret.proxy == Proxy.NO_PROXY || ret.proxy == null) {
                            security.checkConnect(InetAddress.getByName(url.getHost()).getHostAddress(), url.getPort());
                        } else {
                            security.checkConnect(url.getHost(), url.getPort());
                        }
                }
            ret.url = url;
        }
        return ret;
    }

    public static HttpClient New(URL url, Proxy p, InetAddress localAddress, int to) throws IOException {
        return New(url, p, localAddress, to, true);
    }

    public static HttpClient New(URL url, String proxyHost, int proxyPort, InetAddress localAddress, boolean useCache, int to) throws IOException {
        return New(url, newHttpProxy(proxyHost, proxyPort, "http"), localAddress, to, useCache);
    }

    @Override
    protected synchronized void putInKeepAliveCache() {
        if (inCacheOverride) {
            return;
        }
        inCacheOverride = true;
        kac.put(url, null, this);
    }

    @Override
    protected boolean isInKeepAliveCache() {
        return inCacheOverride;
    }

    @Override
    protected Socket createSocket() throws IOException {
        Socket s = super.createSocket();
        if (localAddress != null) {
            InetSocketAddress localSocketAddress = new InetSocketAddress(localAddress, 0);
            s.bind(localSocketAddress);
        }
        return s;
    }
}
