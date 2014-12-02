package com.microsoft.azure.storage.http;

import sun.net.www.protocol.https.Handler;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.*;
import java.util.Objects;

public class AzureHttpsHandler extends Handler {
    private final InetAddress localAddress;

    public AzureHttpsHandler(InetAddress localAddress) {
        this.localAddress = localAddress;
    }

    @Override
    protected URLConnection openConnection(URL u) throws IOException {
        return openConnection(u, null);
    }

    @Override
    protected URLConnection openConnection(URL u, Proxy p) throws IOException {
        HttpsURLConnection conn = (HttpsURLConnection) super.openConnection(u, p);
        if (localAddress != null) {
            conn.setSSLSocketFactory(new BindingSSLSocketFactory(localAddress));
        }
        return conn;
    }

    private static class BindingSSLSocketFactory extends SSLSocketFactory {
        private final SSLSocketFactory delegate;
        private final InetAddress localAddress;

        private BindingSSLSocketFactory(InetAddress localAddress) {
            this.delegate = (SSLSocketFactory) SSLSocketFactory.getDefault();
            this.localAddress = localAddress;
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return delegate.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public Socket createSocket() throws IOException {
            Socket s = delegate.createSocket();
            s.bind(new InetSocketAddress(localAddress, 0));
            return s;
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            Socket s = delegate.createSocket(host, port);
            s.bind(new InetSocketAddress(localAddress, 0));
            return s;
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
            return delegate.createSocket(host, port, localHost, localPort);
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            Socket s = delegate.createSocket(host, port);
            s.bind(new InetSocketAddress(localAddress, 0));
            return s;
        }

        @Override
        public Socket createSocket(InetAddress host, int port, InetAddress localAddress, int localPort) throws IOException {
            return delegate.createSocket(host, port, localAddress, localPort);
        }

        @Override
        public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
            return delegate.createSocket(s, host, port, autoClose);
        }

        @Override
        public int hashCode() {
            return Objects.hash(delegate, localAddress);
        }

        @Override
        public boolean equals(Object that) {
            if (this == that) {
                return true;
            }
            if (that instanceof BindingSSLSocketFactory) {
                BindingSSLSocketFactory thatBindingSSLSocketFactory = (BindingSSLSocketFactory) that;
                return delegate.equals(thatBindingSSLSocketFactory.delegate)
                        && localAddress.equals(thatBindingSSLSocketFactory.localAddress);
            }
            return false;
        }
    }
}
