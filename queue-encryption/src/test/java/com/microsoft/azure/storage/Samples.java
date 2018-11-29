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

import com.microsoft.azure.keyvault.cryptography.SymmetricKey;
import com.microsoft.azure.storage.queue.*;
import com.microsoft.azure.storage.queue.encryption.EncryptedMessageIdURL;
import com.microsoft.azure.storage.queue.encryption.EncryptedMessagesURL;
import com.microsoft.azure.storage.queue.encryption.MessageEncryptionPolicy;
import com.microsoft.azure.storage.queue.models.*;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Single;
import org.junit.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
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
     *  This example shows how to get started using the Azure Storage Queue SDK with encrypted messages for Java.
     */
    @Test
    public void basicExample() throws MalformedURLException, NoSuchAlgorithmException {
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

        // Create Symmetric Key.
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256);
        SecretKey secretKey = keyGen.generateKey();
        SymmetricKey symmetricKey = new SymmetricKey("keyId", secretKey.getEncoded());

        // Create messageEncryptionPolicy.
        MessageEncryptionPolicy mep = new MessageEncryptionPolicy(symmetricKey, null);

        // Create encryptedMessageUrl which allows to enqueue, dequeue and to further manipulate the encrypted queues message.
        EncryptedMessagesURL emu = new EncryptedMessagesURL(qu, mep);

        qu.create()
                .flatMap(queueCreateResponse ->
                        emu.enqueue("This a message"))
                .flatMap(enqueueResponse ->
                        emu.peek(10))
                .doOnSuccess(peekResponse -> {
                    if(!peekResponse.body().get(0).messageText().equals("This a message")) {
                        throw new Exception("The peeked message does not match the enqueued message");
                    }
                })
                .flatMap(messagePeekResponse ->
                        emu.dequeue(1, 10))
                .doOnSuccess(dequeueResponse -> {
                    if(!dequeueResponse.body().get(0).messageText().equals("This a message")) {
                        throw new Exception("The dequeued message does not match the enqueued message");
                    }
                })
                /*
                This will synchronize all the above operations. This is strongly discouraged for use in production as
                it eliminates the benefits of asynchronous IO. We use it here to enable the sample to complete and
                demonstrate its effectiveness.
                */
                .blockingGet();
    }

    @Test
    public void exampleUpdateDeleteMessage() throws MalformedURLException, NoSuchAlgorithmException {
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

        // Create Symmetric Key.
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256);
        SecretKey secretKey = keyGen.generateKey();
        SymmetricKey symmetricKey = new SymmetricKey("keyId", secretKey.getEncoded());

        // Create messageEncryptionPolicy.
        MessageEncryptionPolicy mep = new MessageEncryptionPolicy(symmetricKey, null);

        // Create encryptedMessageUrl which allows to enqueue, dequeue and to further manipulate the encrypted queues message.
        EncryptedMessagesURL emu = new EncryptedMessagesURL(qu, mep);

        qu.create()
                .flatMap(queueCreateResponse ->
                        emu.enqueue("This a message")
                        .flatMap(messageEnqueueResponse -> {
                                EnqueuedMessage enqueuedMessage = messageEnqueueResponse.body().get(0);
                                EncryptedMessageIdURL emidu = emu.createEncryptedMessageIdUrl(enqueuedMessage.messageId());
                                return emidu.update(enqueuedMessage.popReceipt(), 0, "updated message")
                                        .flatMap(updateResponse ->
                                                emu.peek(1)
                                                        .doOnSuccess(messagePeekResponse -> {
                                                            if (!messagePeekResponse.body().get(0).messageText().equals("updated message")) {
                                                                throw new Exception("The peeked message does not match the enqueued message");
                                                            }
                                                        })
                                                        .flatMap(messagePeekResponse2 ->
                                                                emidu.delete(updateResponse.headers().popReceipt()))
                                                                .flatMap(deleteResponse ->
                                                                        emu.peek(1))
                                                                .doOnSuccess(messagePeekResponse2 -> {
                                                                    if(messagePeekResponse2.body().size() != 0) {
                                                                        throw new Exception("The peeked message count should be 0");
                                                                    }
                                                                })
                                                );
                                        })

                ).blockingGet();
    }
}


