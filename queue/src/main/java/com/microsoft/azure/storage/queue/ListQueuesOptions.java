package com.microsoft.azure.storage.queue;

import com.microsoft.azure.storage.queue.models.ListQueuesResponse;

/**
 * Defines options available to configure the behavior of a call to listQueuesSegment on a {@link ServiceURL}
 * object. See the constructor for details on each of the options.
 */
public final class ListQueuesOptions {

    /**
     * An object representing the default options: no details, prefix, or delimiter. Uses the server default for
     * maxResults.
     */
    public static final ListQueuesOptions DEFAULT = new ListQueuesOptions();

    private QueueListingDetails details;

    private String prefix;

    private Integer maxResults;

    /**
     * {@link QueueListingDetails}
     */
    public QueueListingDetails details() {
        return details;
    }

    /**
     * {@link QueueListingDetails}
     */
    public ListQueuesOptions withDetails(QueueListingDetails details) {
        this.details = details;
        return this;
    }

    /**
     * Filters the results to return only queues whose names begin with the specified prefix.
     * May be null to return all the queues.
     */
    public String prefix() {
        return this.prefix;
    }

    /**
     * Filters the results to return only queues whose names begin with the specified prefix.
     * May be null to return all the queues.
     */
    public ListQueuesOptions withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }


    /**
     * Specifies the maximum number of queues to return. If maxresults is not specified,
     * the server will return up to 5,000 items.
     */
    public Integer maxResults() {
        return this.maxResults;
    }

    /**
     * Specifies the maximum number of queues to return. If maxresults is not specified,
     * the server will return up to 5,000 items.
     */
    public ListQueuesOptions withMaxResults(Integer maxResults) {
        this.maxResults = maxResults;
        return this;
    }


}
