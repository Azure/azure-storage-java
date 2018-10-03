/*
 * Copyright Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.azure.storage;

import com.microsoft.azure.storage.queue.*;
import com.microsoft.azure.storage.queue.models.*;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.*;
import io.reactivex.Observable;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.time.OffsetDateTime;
import java.util.*;


public class Samples {
    private String getAccountName() {
        return System.getenv("ACCOUNT_NAME");
    }

    private String getAccountKey() {
        return System.getenv("ACCOUNT_KEY");
    }

    private static final String queuePrefix = "jtq";

    /*
     *  This example shows how to get started using the Azure Storage Queue SDK for Java.
     */
    @Test
    public void basicExample() throws MalformedURLException, InterruptedException{
        String accountName = getAccountName();
        String accountKey = getAccountKey();

        // Use your Storage account's name and key to create a credential object; this is required to sign a SAS.
        SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);

        /*
        Create a request pipeline that is used to process HTTP(S) requests and responses. It requires your account
        credentials. In more advanced scenarios, you can configure telemetry, retry policies, logging, and other
        options. Also you can configure multiple pipelines for different scenarios.
         */
        HttpPipeline pipeline = StorageURL.createPipeline(credential);

        /*
        From the Azure portal, get your Storage account queue service URL endpoint.
        The URL typically looks like this:
         */
        URL u = new URL(String.format(Locale.ROOT, "https://%s.queue.core.windows.net", accountName));

        /*
        Create a URL that references a queue in your Azure Storage account.
        This returns a QueueURL object that wraps the queue's URL and a request pipeline (inherited from serviceURL).
        */
        QueueURL qu = new ServiceURL(u, pipeline).
                createQueueUrl(queuePrefix + String.valueOf(new Random().nextInt(100000) + 1));

        // Creates a queue Single which stages an operation to create a queue.
        Single<QueueCreateResponse> queueCreateSingle = qu.create();

        // Create messageUrl which allows to enqueue, dequeue and to further manipulate the queues message.
        MessagesURL mu = qu.createMessagesUrl();

        queueCreateSingle
                .flatMap(response ->
                        mu.enqueue("This is message 1"))
                .flatMap(response ->
                        mu.enqueue("This is message 2"))
                .flatMap(response ->
                        mu.dequeue(1, 5))
                .doOnSuccess(messageDequeueResponse -> {
                    if (messageDequeueResponse.body().get(0).messageId().startsWith("This is message")){
                        throw new Exception("The dequeued message does not match enqueued message.");
                    }
                })
                .flatMap(response ->
                        mu.dequeue(1, 5))
                .doOnSuccess(messageDequeueResponse -> {
                    if (messageDequeueResponse.body().get(0).messageId().startsWith("This is message")){
                        throw new Exception("The dequeued message does not match enqueued message.");
                    }
                })
                .flatMap(response ->
                        qu.delete())
                /*
                This will synchronize all the above operations. This is strongly discouraged for use in production as
                it eliminates the benefits of asynchronous IO. We use it here to enable the sample to complete and
                demonstrate its effectiveness.
                 */
                .blockingGet();
    }

    /*
     *  This example mimics some arbitrary number of clients continuously sending messages up to a queue in a parallel and
     *  a server dequeuing the messages and processing them.
     */
    @Test
    public void example() throws MalformedURLException, InterruptedException {
        String accountName = getAccountName();
        String accountKey = getAccountKey();

        // Use your Storage account's name and key to create a credential object; this is required to sign a SAS.
        SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);

        /*
        Create a request pipeline that is used to process HTTP(S) requests and responses. It requires your account
        credentials. In more advanced scenarios, you can configure telemetry, retry policies, logging, and other
        options. Also you can configure multiple pipelines for different scenarios.
         */
        HttpPipeline pipeline = StorageURL.createPipeline(credential);

        /*
        From the Azure portal, get your Storage account queue service URL endpoint.
        The URL typically looks like this:
         */
        URL u = new URL(String.format(Locale.ROOT, "https://%s.queue.core.windows.net", accountName));

        /*
        Create a URL that references a queue in your Azure Storage account.
        This returns a QueueURL object that wraps the queue's URL and a request pipeline (inherited from serviceURL).
        */
        QueueURL qu = new ServiceURL(u, pipeline).
                createQueueUrl(queuePrefix + String.valueOf(new Random().nextInt(100000) + 1));

        // Creates a queue Single which stages an operation to create a queue.
        Single<QueueCreateResponse> queueCreateSingle = qu.create();

        // Create messageUrl which allows to enqueue, dequeue and to further manipulate the queues message.
        MessagesURL mu = qu.createMessagesUrl();

        /*
         Create the queue and add the messages. This mimics some arbitrary number of clients continuously sending
         messages up to a queue in parallel and without any coordination between them. Here we enqueue two messages
         for demonstration purposes.
         */
        queueCreateSingle
                .flatMap(response ->
                        mu.enqueue("This is message 1"))
                .flatMap(response ->
                        mu.enqueue("This is message 2"))
                /*
                This will synchronize all the above operations. This is strongly discouraged for use in production
                as it eliminates the benefits of asynchronous IO. We use it here to enable the sample to complete and
                demonstrate its effectiveness.
               */
                .blockingGet();


        /*
        This infinite loop illustrates how a Service will process messages.
        */
        while (true) {
            /*
            Because a server application of this type typically has the dedicated role of dequeuing and processing
            messages, there is no benefit to performing the dequeue operation asynchronously. There is no other work
            to do until we have messages to process, so a blockingGet is acceptable here.
            */
            MessageDequeueResponse response = mu.dequeue(MessagesURL.MAX_NUMBER_OF_MESSAGES_TO_DEQUEUE, 1, null).blockingGet();
            if (response.body().size() == 0) {
                /*
                The queue was empty; sleep a bit and try again. Shorter time means higher costs & less latency to
                process a message. Higher time means lower costs & more latency to process a message. This tradeoff
                is because it's likely there will be more messages to dequeue in a batch if we sleep longer, but this,
                of course, wastes time. We have not begun any asynchronous processing, so a normal Thread.sleep() is
                acceptable
                */
                Thread.sleep(1000);
                continue;
            } else {
                /*
                Here we are processing the messages retrieved from the queue.
                NOTE: The queue does not guarantee FIFO ordering & processing messages in parallel also does
                not preserve FIFO ordering.
                */
                Observable.fromIterable(response.body())
                        .flatMap(dequeuedMessageItem -> {
                            // Process a message.
                            MessageIdURL msgIdUrl = mu.createMessageIdUrl(dequeuedMessageItem.messageId());
                            /*
                            This is message's most-recent pop receipt and this pop receipt is required
                            to perform any operation on the respective message i.e update / delete message.
                            */
                            String popReceipt = dequeuedMessageItem.popReceipt();


                            /*
                             In this example, any message retrieved 4 or more times is assumed to be a poison message.
                             This means that there is something broken about the message which we are apparently unable to fix.
                            */
                            final int poisonMessageDequeThresholdCount = 4;
                            if (dequeuedMessageItem.dequeueCount() > poisonMessageDequeThresholdCount) {
                                // Log the poison message and immediately delete it without attempting to process it.
                                System.out.println("Logging Poison Message " + dequeuedMessageItem.messageId()
                                        + " has dequeue count " + dequeuedMessageItem.dequeueCount());
                                return msgIdUrl.delete(popReceipt).toObservable();
                            }

                            // NOTE: You can examine/use any of the message's other properties as you desire
                            System.out.println("Message Insertion Time " + dequeuedMessageItem.insertionTime().toString());
                            System.out.println("Message Expiration Time " + dequeuedMessageItem.insertionTime().toString());
                            System.out.println("Message Next Visible Time " + dequeuedMessageItem.timeNextVisible().toString());

                            /*
                            OPTIONAL: while processing a message, you can update the message's visibility timeout
                            (to prevent other servers from dequeuing the same message simultaneously) and update the
                            message's text (to prevent some successfully-completed processing from re-executing the
                            next time this message is dequeued).
                            */
                            return msgIdUrl.update(popReceipt, 1, "update message text-1")
                                    .flatMapObservable(updateResponse -> {
                                        // After update, get latest popReceipt to continue processing the message...


                                        // After processing the whole message, delete it from the queue so it won't be dequeued ever again
                                        return msgIdUrl.delete(updateResponse.headers().popReceipt()).toObservable();
                                    });
                        }, 10)
                        /*
                        blockingSubscribe is a synchronization barrier. No code will execute beyond this point until all the messages are
                        processed. There is a tradeoff here in that there will be some down time on some of the CPUs while they all wait
                        for the last message to be processed, but it will mean that we can again retrieve a large number of messages in
                        one requests. It is alternatively possible to set up a workflow in which more messages are immediately retrieved
                        upon a given message's completion, making smaller but more frequent requests with the same implications described
                        above. This, however, is not demonstrated here.
                        */
                        .blockingSubscribe();
                // NOTE: For this example only, break out of the infinite loop so this example terminates
                break;
            }
        }
    }

    /*
     * This example shows how to break a URL into its parts so you can
     * examine and/or change some of its values and then construct a new URL.
     */
    @Test
    public void exampleQueueUrlParts() throws UnknownHostException, MalformedURLException {
        // Let's start with a URL that identifies a queue that also contains a Shared Access Signature (SAS):
        URL u = new URL("https://myaccount.queue.core.windows.net/aqueue/messages/30dd879c-ee2f-11db-8314-0800200c9a66?" +
                "sv=2015-02-21&sr=q&st=2111-01-09T01:42:34.936Z&se=2222-03-09T01:42:34.936Z&sp=rup&sip=168.1.5.60-168.1.5.70&" +
                "spr=https,http&si=myIdentifier&ss=q&srt=o&sig=92836758923659283652983562==");

        // Parse the URL and break it into its constituent parts.
        QueueURLParts parts = URLParser.parse(u);

        // Now, we access the parts (this example prints them).
        System.out.println(String.join("\n",
                parts.host(),
                parts.queueName()));
        System.out.println("");
        SASQueryParameters sas = parts.sasQueryParameters();
        System.out.println(String.join("\n",
                sas.version(),
                sas.startTime().toString(),
                sas.expiryTime().toString(),
                sas.permissions(),
                sas.ipRange().toString(),
                sas.protocol().toString(),
                sas.identifier(),
                sas.services(),
                sas.signature()));

        // We can then change some of the fields and construct a new URL:
        parts.withSasQueryParameters(null)
                .withQueueName("otherqueue")
                .withMessages(false)
                .withMessageId("");

        // Construct a new URL from the parts.
        URL newUrl = parts.toURL();
        System.out.println(newUrl);
    }

    /*
     *  This example shows how to create and use an Azure Storage account Shared Access Signature (SAS).
     */
    @Test
    public void exampleAccountSASSignatureValues() throws UnknownHostException, InvalidKeyException, MalformedURLException {
        // From the Azure portal, get your Storage account's name and account key.
        String accountName = getAccountName();
        String accountKey = getAccountKey();

        // Use your Storage account's name and key to create a credential object; this is required to sign a SAS.
        SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);

        // Set the desired SAS signature values and sign them with the shared key credentials to get the SAS query parameters.
        AccountSASSignatureValues values = new AccountSASSignatureValues();
        values.withProtocol(SASProtocol.HTTPS_ONLY)
                .withExpiryTime(OffsetDateTime.now().plusDays(2));

        // Set the desired permissions which you expect the SAS to provided.
        AccountSASPermission permissions = new AccountSASPermission().withRead(true);

        AccountSASService service = new AccountSASService().withQueue(true);

        values.withPermissions(permissions.toString());
        values.withServices(service.toString());

        AccountSASResourceType resourceType = new AccountSASResourceType().withContainer(true);
        values.withResourceTypes(resourceType.toString());

        SASQueryParameters params = values.generateSASQueryParameters(credential);
        String encodedParams = params.encode();

        String urlToShare = String.format(Locale.ROOT, "https://%s.queue.core.windows.net?%s", accountName, encodedParams);
        // At this point, you can send the urlToSendToSomeone to someone via email or any other mechanism you choose.

        // ************************************************************************************************

        // When someone receives the URL, they access the SAS-protected resource with code like this:
        URL u = new URL(urlToShare);

        /*
        Create an ServiceURL object that wraps the service URL (and its SAS) and a pipeline.
        When using a SAS URLs, anonymous credentials are required.
        */
        ServiceURL serviceURL = new ServiceURL(u, StorageURL.createPipeline());
        // Now, you can use this serviceURL just like any other to make requests of the resource.

        // You can parse a URL into its constituent parts:
        QueueURLParts parts = URLParser.parse(serviceURL.toURL());
        System.out.println(String.format(Locale.ROOT, "SAS Protocol=%s\n", parts.sasQueryParameters().protocol()));
        System.out.println(String.format(Locale.ROOT, "SAS Permissions=%s\n", parts.sasQueryParameters().permissions()));
        System.out.println(String.format(Locale.ROOT, "SAS Services=%s\n", parts.sasQueryParameters().services()));
        System.out.println(String.format(Locale.ROOT, "SAS ResourceTypes=%s\n", parts.sasQueryParameters().resourceTypes()));
    }

    /*
     *   This examples shows how to list the queues in an Azure Storage service account.
     */
    @Test
    public void exampleQueuesListSegment() throws MalformedURLException, InvalidKeyException {
        // From the Azure portal, get your Storage account file service URL endpoint.
        String accountName = getAccountName();
        String accountKey = getAccountKey();

        // Use your Storage account's name and key to create a credential object; this is required to sign a SAS.
        SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);

        /*
        Create a request pipeline that is used to process HTTP(S) requests and responses. It requires your account
        credentials. In more advanced scenarios, you can configure telemetry, retry policies, logging, and other
        options. Also you can configure multiple pipelines for different scenarios.
         */
        HttpPipeline pipeline = StorageURL.createPipeline(credential);

        /*
        From the Azure portal, get your Storage account queue service URL endpoint.
        The URL typically looks like this:
         */
        URL u = new URL(String.format(Locale.ROOT, "https://%s.queue.core.windows.net", accountName));

        // Create Service URL object that wraps the service URL and a request pipeline
        ServiceURL su = new ServiceURL(u, pipeline);

        // Create a few queue url so that we can create them in order to list them.
        QueueURL q1 = su.createQueueUrl("jtq-test-queue-1");
        QueueURL q2 = su.createQueueUrl("jtq-test-queue-2");

        // Create set of metadata which we can use while creating the queue.
        Metadata metadata1 = new Metadata();
        metadata1.put("key1", "value1");

        Metadata metadata2 = new Metadata();
        metadata2.put("key1", "value1");

        // create a queue with metadata so that we can list them.
        q1.create(metadata1, null)
                .flatMap(response ->
                        // create another queue with metadata so that we can list them.
                        q2.create(metadata2, null))
                .flatMap(response ->
                        /*List the queue in our account; since the account may hold huge number of queues,
                         listing is done one segment at a time. */
                        su.listQueuesSegment(null, new ListQueuesOptions().withMaxResults(1), null))
                .flatMap(response ->
                        // The asynchronous requests require we use recursion to continue our listing.
                        listQueuesHelper(su, response))
                .flatMap(response ->
                        // Deletion is not required, it is done in sample for cleanup.
                        q1.delete(null))
                .flatMap(response ->
                        // Deletion is not required, it is done in sample for cleanup.
                        q2.delete(null))
                /*
                This will synchronize all the above operations. This is strongly discouraged for use in production as
                it eliminates the benefits of asynchronous IO. We use it here to enable the sample to complete and
                demonstrate its effectiveness.
                */
                .blockingGet();
    }

    // <list_queues_flat_helper>
    private Single<ServiceListQueuesSegmentResponse> listQueuesHelper(
            ServiceURL serviceURL, ServiceListQueuesSegmentResponse response) {

        // Process the queue returned in this result segment (if the segment is empty, queueItems will null.
        if (response.body().queueItems() != null) {
            for (QueueItem q : response.body().queueItems()) {
                System.out.println("Queue name: " + q.name());
            }
        }

        // If there is not another segment, return this response as the final response.
        if (response.body().nextMarker() == null) {
            return Single.just(response);
        } else {
            /*
             IMPORTANT: listQueuesSegment returns the start of the next segment; you MUST use this to get the next
             segment (after processing the current result segment
             */
            String nextMarker = response.body().nextMarker();

            /*
            The presence of the marker indicates that there are more queue to list, so we make another call to
            listQueuesSegment and pass the result through this helper function.
             */
            return serviceURL.listQueuesSegment(nextMarker, new ListQueuesOptions().withMaxResults(1), null)
                    .flatMap(serviceListQueuesSegmentResponse ->
                            listQueuesHelper(serviceURL, serviceListQueuesSegmentResponse));
        }
    }

    @Test
    public void exampleQueuesSetAccessPolicy() throws MalformedURLException, InvalidKeyException {
        String accountName = getAccountName();
        String accountKey = getAccountKey();

        // Use your Storage account's name and key to create a credential object; this is required to sign a SAS.
        SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);

        /*
        Create a request pipeline that is used to process HTTP(S) requests and responses. It requires your account
        credentials. In more advanced scenarios, you can configure telemetry, retry policies, logging, and other
        options. Also you can configure multiple pipelines for different scenarios.
         */
        HttpPipeline pipeline = StorageURL.createPipeline(credential, new PipelineOptions());

        /*
        From the Azure portal, get your Storage account queue service URL endpoint.
        The URL typically looks like this:
         */
        URL u = new URL(String.format(Locale.ROOT, "https://%s.queue.core.windows.net", accountName));

        // Create Service URL object that wraps the service URL and a request pipeline
        ServiceURL su = new ServiceURL(u, pipeline);

        // Create a queue url so that we can create them in order to set access policy for the queue created.
        QueueURL qu = su.createQueueUrl("jtq1-test-queue-access-policies");

        List<SignedIdentifier> identifiers = new ArrayList<>();

        // Create the list of Signed Identifiers
        identifiers.add(new SignedIdentifier().withId("Managers").withAccessPolicy(new AccessPolicy().withPermission("ra")));
        identifiers.add(new SignedIdentifier().withId("Engineers").withAccessPolicy(new AccessPolicy().withPermission("r")));

        qu.create(null, null)
                .flatMap(createResponse ->
                        //Set Access Policy
                        qu.setAccessPolicy(identifiers, null))
                .flatMap(setPolicyResponse ->
                        // Get the access policy
                        qu.getAccessPolicy(null))
                .doOnSuccess(getPolicyResponse -> {
                    for (SignedIdentifier si : getPolicyResponse.body()) {
                        System.out.println(si.id() + " " + si.accessPolicy().permission());
                    }
                })
                .flatMap(getPolicyResponse ->
                        // Delete the queue
                        qu.delete(null))
                /*
                This will synchronize all the above operations. This is strongly discouraged for use in production as
                it eliminates the benefits of asynchronous IO. We use it here to enable the sample to complete and
                demonstrate its effectiveness.
                */
                .blockingGet();
    }


    @Test
    public void exampleQueueMessageClear() throws MalformedURLException, InvalidKeyException {
        // Get the account name and account key from the environment variables
        String accountName = getAccountName();
        String accountKey = getAccountKey();

        // Use your Storage account's name and key to create a credential object; this is required to sign a SAS.
        SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);

        /*
        Create a request pipeline that is used to process HTTP(S) requests and responses. It requires your account
        credentials. In more advanced scenarios, you can configure telemetry, retry policies, logging, and other
        options. Also you can configure multiple pipelines for different scenarios.
         */
        HttpPipeline pipeline = StorageURL.createPipeline(credential, new PipelineOptions());

        /*
        From the Azure portal, get your Storage account queue service URL endpoint.
        The URL typically looks like this:
         */
        URL u = new URL(String.format(Locale.ROOT, "https://%s.queue.core.windows.net", accountName));

        // Create Service URL object that wraps the service URL and a request pipeline
        ServiceURL su = new ServiceURL(u, pipeline);

        QueueURL qu = su.createQueueUrl("jtq1-test-queue-message-clear");

        MessagesURL mu = qu.createMessagesUrl();

        // Create the queue
        qu.create(null, null)
                .flatMap(createQueueResponse ->
                        mu.enqueue("Text 1", null, null, null))
                .flatMap(messageEnqueueResponse ->
                        mu.enqueue("Text 2", null, null, null))
                .flatMap(messageEnqueueResponse ->
                        qu.getProperties(null))
                .flatMap(queueGetPropertiesResponse ->
                        mu.clear(null))
                .flatMap(messageClearResponse ->
                        qu.getProperties(null))
                .flatMap(queueGetPropertiesResponse ->
                        qu.delete(null))
                /*
                This will synchronize all the above operations. This is strongly discouraged for use in production as
                it eliminates the benefits of asynchronous IO. We use it here to enable the sample to complete and
                demonstrate its effectiveness.
                */
                .blockingGet();
    }

    @Test
    public void exampleQueueGetProperties() throws MalformedURLException, InvalidKeyException {
        // Get the account name and account key from the environment variables
        String accountName = getAccountName();
        String accountKey = getAccountKey();

        // Use your Storage account's name and key to create a credential object; this is required to sign a SAS.
        SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);

        /*
        Create a request pipeline that is used to process HTTP(S) requests and responses. It requires your account
        credentials. In more advanced scenarios, you can configure telemetry, retry policies, logging, and other
        options. Also you can configure multiple pipelines for different scenarios.
         */
        HttpPipeline pipeline = StorageURL.createPipeline(credential, new PipelineOptions());

        /*
        From the Azure portal, get your Storage account queue service URL endpoint.
        The URL typically looks like this:
         */
        URL u = new URL(String.format(Locale.ROOT, "https://%s.queue.core.windows.net", accountName));

        // Create Service URL object that wraps the service URL and a request pipeline
        ServiceURL su = new ServiceURL(u, pipeline);

        QueueURL qu = su.createQueueUrl("jtq1-test-queue-get-properties");

        Metadata metadata = new Metadata();
        metadata.put("key1", "value1");

        // Create the queue
        qu.create(metadata, null)
                .flatMap(response ->
                        qu.getProperties(null))
                .doOnSuccess(response -> {
                    for (Map.Entry<String, String> entry : response.headers().metadata().entrySet()) {
                        System.out.println("Key = " + entry.getKey() + " Value = " + entry.getValue());
                    }
                    metadata.put("key2", "value2");
                })
                .flatMap(response ->
                        qu.setMetadata(metadata, null))
                .flatMap(response ->
                        qu.getProperties(null))
                .doOnSuccess(response -> {
                    System.out.println("Updated Metadata");
                    for (Map.Entry<String, String> entry : response.headers().metadata().entrySet()) {
                        System.out.println("Key = " + entry.getKey() + " Value = " + entry.getValue());
                    }
                })
                .flatMap(response ->
                        qu.delete(null))
                /*
                This will synchronize all the above operations. This is strongly discouraged for use in production as
                it eliminates the benefits of asynchronous IO. We use it here to enable the sample to complete and
                demonstrate its effectiveness.
                */
                .blockingGet();
    }
}


