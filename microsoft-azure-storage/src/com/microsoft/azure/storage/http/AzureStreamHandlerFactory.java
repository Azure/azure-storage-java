package com.microsoft.azure.storage.http;

import com.microsoft.azure.storage.Constants;

import java.net.InetAddress;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AzureStreamHandlerFactory implements URLStreamHandlerFactory {
    private static final AzureStreamHandlerFactory instance = new AzureStreamHandlerFactory();

    private Map<InetAddress, URLStreamHandler> httpHandlerMap = new ConcurrentHashMap<InetAddress, URLStreamHandler>();
    private Map<InetAddress, URLStreamHandler> httpsHandlerMap = new ConcurrentHashMap<InetAddress, URLStreamHandler>();

    public static AzureStreamHandlerFactory getInstance() {
        return instance;
    }

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
        return createURLStreamHandler(protocol, null);
    }

    public URLStreamHandler createURLStreamHandler(String protocol, InetAddress localAddress) {
        if (Constants.HTTP.equalsIgnoreCase(protocol)) {
            return getHandler(localAddress, false);
        }
        if (Constants.HTTPS.equalsIgnoreCase(protocol)) {
            return getHandler(localAddress, true);
        }
        return null;
    }

    private URLStreamHandler getHandler(InetAddress addr, boolean isHttps) {
        URLStreamHandler handler;
        if (isHttps) {
            handler = httpsHandlerMap.get(addr);
        } else {
            handler = httpHandlerMap.get(addr);
        }
        if (handler != null) {
            return handler;
        }

        if (isHttps) {
            handler = new AzureHttpsHandler(addr);
            httpsHandlerMap.put(addr, handler);
        } else {
            handler = new AzureHttpHandler(addr);
            httpHandlerMap.put(addr, handler);
        }

        return handler;
    }
}
