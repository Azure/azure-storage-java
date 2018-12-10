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

package com.microsoft.azure.storage

import com.microsoft.azure.storage.queue.AccountSASPermission
import com.microsoft.azure.storage.queue.AccountSASSignatureValues
import com.microsoft.azure.storage.queue.Constants
import com.microsoft.azure.storage.queue.IPRange
import com.microsoft.azure.storage.queue.IPStyleEndPointInfo
import com.microsoft.azure.storage.queue.QueueSASPermission
import com.microsoft.azure.storage.queue.QueueURLParts
import com.microsoft.azure.storage.queue.SASProtocol
import com.microsoft.azure.storage.queue.ServiceSASSignatureValues
import com.microsoft.azure.storage.queue.StorageException
import com.microsoft.azure.storage.queue.URLParser
import com.microsoft.azure.storage.queue.Utility
import com.microsoft.azure.storage.queue.models.StorageErrorCode
import spock.lang.Unroll

import java.time.OffsetDateTime
import java.time.ZoneOffset

class HelperTest extends APISpec {

    def "responseError"() {
        when:
        primaryServiceURL.listQueuesSegment("garbage", null, null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.errorCode() == StorageErrorCode.OUT_OF_RANGE_INPUT
        e.statusCode() == 400
        e.message().contains("One of the request inputs is out of range")
        e.getMessage().contains("<?xml") // Ensure that the details in the payload are printable
    }

    def "serviceSasSignatures"() {
        when:
        def v = new ServiceSASSignatureValues()
        if (permissions != null) {
            def p = new QueueSASPermission()
            p.withRead(true)
            v.withPermissions(p.toString())
        }
        v.withStartTime(startTime)
                .withExpiryTime(expiryTime)
                .withQueueName("q")
        if (ipRange != null) {
            def ipR = new IPRange()
            ipR.withIpMin("ip")
            v.withIpRange(ipR)
        }
        v.withIdentifier(identifier)
                .withProtocol(protocol)

        def token = v.generateSASQueryParameters(primaryCreds)

        then:
        token.signature() == primaryCreds.computeHmac256(expectedStringToSign)

        /*
        We don't test the queueName properties because canonicalized resource is always added as at least
        /blob/accountName. We test canonicalization of resources later. Again, this is not to test a fully functional
        sas but the construction of the string to sign.
         */
        where:
        permissions              | startTime                                                 | expiryTime                                                | identifier | ipRange       | protocol               || expectedStringToSign
        new QueueSASPermission() | null                                                      | null                                                      | null       | null          | null                   || "r\n\n\n" + "/queue/" + primaryCreds.getAccountName() + "/q\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION
        null                     | OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC) | null                                                      | null       | null          | null                   || "\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\n\n/queue/" + primaryCreds.getAccountName() + "/q\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION
        null                     | null                                                      | OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC) | null       | null          | null                   || "\n\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\n/queue/" + primaryCreds.getAccountName() + "/q\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION
        null                     | null                                                      | null                                                      | "id"       | null          | null                   || "\n\n\n/queue/" + primaryCreds.getAccountName() + "/q\nid\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION
        null                     | null                                                      | null                                                      | null       | new IPRange() | null                   || "\n\n\n/queue/" + primaryCreds.getAccountName() + "/q\n\nip\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION
        null                     | null                                                      | null                                                      | null       | null          | SASProtocol.HTTPS_ONLY || "\n\n\n/queue/" + primaryCreds.getAccountName() + "/q\n\n\n" + SASProtocol.HTTPS_ONLY + "\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION
        null                     | null                                                      | null                                                      | null       | null          | null                   || "\n\n\n/queue/" + primaryCreds.getAccountName() + "/q\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION
    }

    def "accountSasSignatures"() {
        when:
        def v = new AccountSASSignatureValues()
        def p = new AccountSASPermission()
                .withRead(true)
        v.withPermissions(p.toString())
                .withServices("q")
                .withResourceTypes("o")
                .withStartTime(startTime)
                .withExpiryTime(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))
        if (ipRange != null) {
            def ipR = new IPRange()
            ipR.withIpMin("ip")
            v.withIpRange(ipR)
        }
        v.withProtocol(protocol)
        def token = v.generateSASQueryParameters(primaryCreds)

        then:
        token.signature() == primaryCreds.computeHmac256(expectedStringToSign)

        where:
        startTime                                                 | ipRange       | protocol               || expectedStringToSign
        OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC) | null          | null                   || primaryCreds.getAccountName() + "\nr\nq\no\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n"
        null                                                      | new IPRange() | null                   || primaryCreds.getAccountName() + "\nr\nq\no\n\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\nip\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n"
        null                                                      | null          | SASProtocol.HTTPS_ONLY || primaryCreds.getAccountName() + "\nr\nq\no\n\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\n\n" + SASProtocol.HTTPS_ONLY + "\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n"

    }

    @Unroll
    def "AccountSASPermissions toString"() {
        setup:
        def perms = new AccountSASPermission()
        perms.withRead(read)
                .withWrite(write)
                .withDelete(delete)
                .withList(list)
                .withAdd(add)
                .withCreate(create)
                .withUpdate(update)
                .withProcessMessages(process)

        expect:
        perms.toString() == expectedString

        where:
        read  | write | delete | list  | add   | create | update | process || expectedString
        true  | false | false  | false | false | false  | false  | false   || "r"
        false | true  | false  | false | false | false  | false  | false   || "w"
        false | false | true   | false | false | false  | false  | false   || "d"
        false | false | false  | true  | false | false  | false  | false   || "l"
        false | false | false  | false | true  | false  | false  | false   || "a"
        false | false | false  | false | false | true   | false  | false   || "c"
        false | false | false  | false | false | false  | true   | false   || "u"
        false | false | false  | false | false | false  | false  | true    || "p"
        true  | true  | true   | true  | true  | true   | true   | true    || "rwdlacup"
    }

    def "Queue Url Parts host IP Style test"() {
        when:
        def q = new QueueURLParts()
                .withScheme("http")
                .withHost(host)
                .withQueueName("aqueue")
                .withIPEndPointStyleInfo(new IPStyleEndPointInfo()
                .withAccountName("accountname")
                .withPort(port))
        then:
        q.toURL().toString().equals(expectedUrl)

        where:
        host           | port || expectedUrl
        "105.232.1.23" | null || "http://105.232.1.23/accountname/aqueue"
        "105.232.1.23" | 80   || "http://105.232.1.23:80/accountname/aqueue"
    }

    @Unroll
    def "IP Style Host Url parse test"() {
        when:
        def u = new URL(url)
        def q = URLParser.parse(u)

        then:
        q.scheme() == scheme
        q.host() == host
        if (q.ipEndPointStyleInfo() != null) {
            assert q.ipEndPointStyleInfo().port() == port
            assert q.ipEndPointStyleInfo().accountName() == accountname
        }
        q.queueName() == queuename
        q.messages() == messages
        q.messageId() == messageId

        where:
        url                                                               || scheme | host           | port | accountname   | queuename | messages | messageId
        "http://105.232.1.23/accountname/aqueue"                          || "http" | "105.232.1.23" | null | "accountname" | "aqueue"  | false    | null
        "http://105.232.1.23:80/accountname/aqueue"                       || "http" | "105.232.1.23" | 80   | "accountname" | "aqueue"  | false    | null
        "http://105.232.1.23/accountname/aqueue/messages"                 || "http" | "105.232.1.23" | null | "accountname" | "aqueue"  | true     | null
        "http://105.232.1.23/accountname/aqueue/messages/randommessageId" || "http" | "105.232.1.23" | null | "accountname" | "aqueue"  | true     | "randommessageId"
        "http://105.232.1.23/accountname"                                 || "http" | "105.232.1.23" | null | "accountname" | ""      | false    | null
    }
}
