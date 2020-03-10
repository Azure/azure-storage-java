// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.storage.core;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * RESERVED FOR INTERNAL USE.
 *
 * This type is used to help work around a bug in the JDK where connection timeouts are not honored on a retried
 * request. In other words, if a customer set a timeout on an operation, this timeout is only ever respected on the
 * first attempt at the request. Retries will cause a different underlying connection implementation to be loaded that
 * will ignore the timeout parameter. Therefore, requests can potentially hang forever if the connection is broken
 * after these retries.
 *
 * Enabling keep-alive timeouts acts as a fallback in these scenarios so that, even if the operation timeout is ignored,
 * the socket will still eventually timeout and the request will be cancelled. We enable keep alive timeouts via a
 * wrapper implementation of a SocketFactory. We use a default socket factory to get sockets from the system and then
 * simply set the keep-alive option to true before returning to the client. This factory will be set on the
 * HttpsUrlConnection objects.
 */
public class KeepAliveSocketFactory extends SSLSocketFactory {
    private SSLSocketFactory delegate;

    KeepAliveSocketFactory(SSLSocketFactory delegate) {
        this.delegate = delegate;
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
    public Socket createSocket(Socket socket, String s, int i, boolean b) throws IOException {
        Socket ret = delegate.createSocket(socket, s, i, b);
        ret.setKeepAlive(true);
        return ret;
    }

    @Override
    public Socket createSocket(String s, int i) throws IOException, UnknownHostException {
        Socket ret = delegate.createSocket(s, i);
        ret.setKeepAlive(true);
        return ret;
    }

    @Override
    public Socket createSocket(String s, int i, InetAddress inetAddress, int i1) throws IOException, UnknownHostException {
        Socket ret = delegate.createSocket(s, i, inetAddress, i1);
        ret.setKeepAlive(true);
        return ret;
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i) throws IOException {
        Socket ret = delegate.createSocket(inetAddress, i);
        ret.setKeepAlive(true);
        return ret;
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i, InetAddress inetAddress1, int i1) throws IOException {
        Socket ret = delegate.createSocket(inetAddress, i, inetAddress1, i1);
        ret.setKeepAlive(true);
        return ret;
    }
}
