package com.microsoft.azure.storage.blob;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


/**
 * Wrapped to get byte buffered exposed to manageable classes that prevents unnecessary byte buffer copy in memory.
 *
 */
public class GettableByteArrayOutputStream extends ByteArrayOutputStream {

    public byte[] getByteArray() {
        return this.buf;
    }


    @Override
    public void close() throws IOException {

        super.close();
        this.buf = null;
    }
}
