package com.microsoft.azure.storage.blob;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;


public final class BlobByteArrayOutputStream extends ByteArrayOutputStream {

    BlobByteArrayOutputStream() {

        super();
    }


    /**
     * Return a inputstream which would avoid buffer copy.
     *
     * @return ByteArrayInputStream backed by internal array
     */
    public ByteArrayInputStream getInputStream() {

        ByteArrayInputStream bin = new ByteArrayInputStream(this.buf, 0, count);
        this.reset();
        this.buf = null;
        return bin;
    }
}
