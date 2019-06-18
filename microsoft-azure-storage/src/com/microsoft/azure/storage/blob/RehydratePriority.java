package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.core.Utility;

import java.util.Locale;

/**
 * The rehydrate priority of the blob.
 */
public enum RehydratePriority {

    /**
     * The rehydrate priority is high.
     */
    HIGH,

    /**
     * The rehydrate priority is standard.
     */
    STANDARD;

    /**
     * Parses a rehydrate priority from the given string.
     *
     * @param rehydratePriorityString
     *        A <code>String</code> which represents the rehydrate priority.
     *
     * @return A <code>RehydratePriority</code> value that represents the rehydrate priority.
     */
    protected static RehydratePriority parse(final String rehydratePriorityString) {
        if (Utility.isNullOrEmpty(rehydratePriorityString)) {
            return STANDARD;
        }
        else if ("high".equals(rehydratePriorityString.toLowerCase(Locale.US))) {
            return HIGH;
        }
        else if ("standard".equals(rehydratePriorityString.toLowerCase(Locale.US))) {
            return STANDARD;
        }
        return STANDARD;
    }
}
