package com.microsoft.azure.storage.queue.encryption;

import java.util.Locale;

/**
 * RESERVED FOR INTERNAL USE. A class which provides utility methods.
 */
final class Utility {

    /**
     * Asserts that a value is not <code>null</code>.
     *
     * @param param
     *         A {@code String} that represents the name of the parameter, which becomes the exception message
     *         text if the <code>value</code> parameter is <code>null</code>.
     * @param value
     *         An <code>Object</code> object that represents the value of the specified parameter. This is the value
     *         being asserted as not <code>null</code>.
     */
    static void assertNotNull(final String param, final Object value) {
        if (value == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "The argument must not be null or an empty string. Argument name: %s.", param));
        }
    }
}
