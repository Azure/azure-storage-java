# Azure Storage Java V12 Changes &amp; Roadmap

This document is directed towards customers of Azure Storage who have engaged with either the v8 (synchronous) or v10 (asynchronous) versions of the Java SDK. As part of responding to the feedback we&#39;ve received on these SDKs, we&#39;ve determined the best course of action is to fully fix the problems encountered, rather than try to incrementally improve a foundation that had issues. Unfortunately, this will require a few breaking changes as we course correct. The rest of this document covers:

- Why we broke from the original architecture in the first place
- Why we have decided it is necessary to break again
- Why `com.azure.storage` v12 represents the culmination of our SDK efforts so far
- How we will support our customers going forward to ensure they that have the best Cloud Storage experience

# From V8 to V10

After several years of experience with the CloudBlob model of v1-8, we realized there were crucial problems with this SDK as scale and complexity continued to grow:

- The CloudBlob model is not thread safe and much of its behavior is opaque to users. Java applications very often have quite a few threads running, so offering a core type that is not thread safe caused many problems for customers.
- The object-oriented caching behavior frequently made it unclear what was stored locally and what was consistent with the remote blob. It wasn&#39;t clear when customers had an up-to-date view of the data and when requests were happening behind the scenes.
- The package size was large. Because all three Storage services (blob, queue, file) were in one artifact, many customers found the dependency was unnecessarily large when only working with one service.
- The SDK featured purely synchronous IO. While we know that many customers still want sync apis, some are also moving towards asynchronous IO to take advantage of possible performance gains. It is important to us that we position ourselves to be successful both with current standards and with the future of Java development.

# From V10 to V12

While v10 and v11 took a respectable stab at ameliorating the issues with v8, it had several problems of its own that grew increasingly apparent as more customers adopted it. The introduction of v12 comes as a result of the following problems:

- An unstable networking stack. Any customer who used these SDKs with a larger workload encountered sporadic &quot;Connection Reset By Peer&quot; exceptions.  This exception was a result of a custom Rx-Netty implementation that simply did not have the maturity needed to support larger scale applications, especially ones that followed a sync-over-async pattern.
- Lack of friendly synchronous support. For any customer that did not already have a purely asynchronous workflow in their application, every API became a sync-over-async call. In addition to contributing to the problems in the networking stack, the blocking was verbose and error prone (as forgetting it would result in no IO operation being sent).
- Architectural mistakes. The core types of the SDK—URL types like BlobURL—while true to their function, were clunky and unintuitive. Additionally, the bare-bones naming conventions led to several key features being &quot;hidden&quot;. It was not discoverable how to do something as fundamental as create a SAS token because there was no generateSAS method, and figuring how to attach a SAS to a URL was yet another problem. The TransferManager type similarly &quot;hid&quot; very important file transfer methods.
- Lack of convenience. It was a very austere API surface that strictly adhered to the REST API. Methods like `exists()` did not exist. While the SDK did not prohibit customers from doing something functionally equivalent to an exists call by calling getProperties and handling a 404, the purpose of an SDK is arguably to provide exactly this kind of convenience, and v10/11 failed in this regard.

# V12: The best of both worlds

Listening to your feedback in conjunction with an effort to unify the Azure SDK experience across services led to us deciding that, despite the upgrade pain for existing customers, this second upgrade would ultimately be the right thing to do in the long run. We have taken the cumulative learnings of versions 1-11 to produce an API that is stable, intuitive, and efficient. Some of the highlights include:

- Side-by-side sync and async apis. Each type (Container, Blob, etc.) has both a sync and async client. It is easy and intuitive to use whichever one is preferable in a given context without having to bother about clutter from the other. Further, because these apis live in the same package and have very similar signatures, it is much easier for customers to gradually upgrade from sync to async should they so desire.
- Several key features from v8 that were missing in v10, notably, BlobInputStream and BlobOutputStream. With the addition of sync apis, BlobInputStream and BlobOutputStream fit nicely within this story. Customers who have long depended on these types will find the same interface and will require minimal upgrade. We will also be adding Client-Side Encryption as we bring the library to full parity.
- Thread-safety and immutability. It is still safe to share clients across threads, and there should still be no confusion about when the client is in sync with the service as there is no state maintained on the client. State may be returned as the result of a method call, but there is no pretense of storing state on a client that could be immediately invalid.
- A reactive framework update, from RxJava to Reactor. The biggest payoff here is the switch to Reactor and ReactorNetty. Both Reactor and Reactor-Netty are much more mature than the ReactiveX + custom-ReactiveX-Netty-interface we were using. Both Reactor and ReactorNetty are maintained by Spring and support other large projects, making them safer and more modern, while similar to what was present in v10.
- An extension packages model to keep the package size small. The three Azure Storage services will be in different artifacts. Similarly, features like Client-Side Encryption will be offered in artifacts separate from the core sdk components, which will reduce the footprint of Azure Storage wherever possible.
- Perf increases over purely synchronous HTTP Clients (e.g. OkHttp). In our long running stress tests, we have observed 3x better performance using Reactor-Netty over OkHttp when using our sync apis. The async apis perform even better. We will publish more detailed performance results before we GA v12.
- Conformance to Azure-wide standards. The new API surface follows guidelines that are standardizing the experience for all Azure SDKs. This will give a coherent story for any developers working within the Azure ecosystem. These APIs should be idiomatic, intuitive, and convenient for Java developers of all experience levels.

# Our Long Term Plan

It has always been our intention and highest priority to provide the best experience and tools for our customers. While we have made some mistakes in the recent past, we are still committed to this goal. Every effort we make is ultimately to empower you to build your products better. To that end, you may expect from us:

- A more stable API. Going forward, we will be making a concerted effort to minimize even minor breaks. This new API surface represents the sum of our learnings and discussions with you and is our long-term strategy.
- Support for your existing applications. Security fixes and bugfixes will continue to be made in every version of the library. Features will continue to be added to both the v8 style and the v12 style API surfaces in concert with the Azure Storage Service.
- Rapid and honest communication over Github.  It will remain a priority for us to answer questions on Github and discuss bugs/feature requests. We are also happy to discuss contributions. A strong community of support and collaboration is important to us.
- Making upgrade as easy as possible. We will be providing more detailed documentation about the upgrade paths, including several samples that give a side-by-side comparison for v8 and v12 code. Furthermore, because we are shipping v12 in a different package and artifact (com.azure vs com.microsoft.azure), side-by-side loading should be very simple to do so customers can upgrade at their own pace.

# Thank you

Thank you for your feedback in the past and going forward. We apologize for any inconvenience this has caused. However, we strongly believe that these updates will allow a path towards greater success. Please feel free to contact us on Github with any suggestions.
