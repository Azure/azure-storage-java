# Microsoft Azure Storage SDK v7 for Java

This project provides a client library in Java that makes it easy to consume Microsoft Azure Storage services. For documentation, please see the Microsoft Azure [Java Developer Center](http://azure.microsoft.com/en-us/develop/java/) and the [Java client library reference for Azure Storage](https://docs.microsoft.com/en-us/java/api/overview/azure/storage).

This README page is a reference to the SDK v7. For the new asynchronous SDK v10, go to [Storage SDK v10 for Java (Preview)](https://github.com/Azure/azure-storage-java/tree/New-Storage-SDK-V10-Preview).

| SDK Name      | Version       | Description | Maven/API Reference Links |
| ------------- | ------------- | ----------- | ----- |
| [Storage SDK v10 for Java](https://github.com/Azure/azure-storage-java/tree/New-Storage-SDK-V10-Preview)  | v10-Preview  | The next generation async Storage SDK (Blob only) | [Maven](https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage-blob) - [Reference](https://docs.microsoft.com/en-us/java/api/storage/client?view=azure-java-preview) |
| [Storage SDK v7 for Java](https://github.com/Azure/azure-storage-java)  | v7  | Legacy Storage SDK (sync only) | [Maven](https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage) - [Reference](https://docs.microsoft.com/en-us/java/api/storage/client?view=azure-java-stable)|
| [Storage SDK for Android](https://github.com/Azure/azure-storage-android) | v2 | Storage SDK for Android | [Maven](https://mvnrepository.com/artifact/com.microsoft.azure.android/azure-storage-android) - [Reference](http://azure.github.io/azure-storage-android/)
| [Azure Management Libraries for Java](https://github.com/Azure/azure-libraries-for-java) | v1 | Management libraries including Storage Resource Provider APIs | [Maven](https://mvnrepository.com/artifact/com.microsoft.azure/azure-mgmt-resources) - [Reference](http://azure.github.io/azure-storage-android/)|

# Features
  * Blob
      * Create/Read/Update/Delete containers
      * Create/Read/Update/Delete blobs
      * Advanced Blob Operations
  * Queue
      * Create/Delete Queues
      * Insert/Peek Queue Messages
      * Advanced Queue Operations
  * Table
      * Create/Read/Update/Delete tables
      * Create/Read/Update/Delete entities
      * Batch operations
      * Advanced Table Operations

# Getting Started

## Download
### Option 1: Via Maven

To get the binaries of this library as distributed by Microsoft, ready for use within your project, you can use Maven.

```xml
<dependency>
	<groupId>com.microsoft.azure</groupId>
	<artifactId>azure-storage</artifactId>
	<version>8.0.0</version>
</dependency>
```

### Option 2: Source Via Git

To get the source code of the SDK via git just type:

    git clone git://github.com/Azure/azure-storage-java.git
    cd ./azure-storage-java
    mvn compile

### Option 3: Source Zip

To download a copy of the source code, click "Download ZIP" on the right side of the page or click [here](https://github.com/Azure/azure-storage-java/archive/master.zip). Unzip and navigate to the microsoft-azure-storage folder.

## Minimum Requirements

* Java 1.7+
* [Jackson-Core](https://github.com/FasterXML/jackson-core) is used for JSON parsing. 
* (Optional) [SLF4J](http://www.slf4j.org/) is a logging facade.
* (Optional) [SLF4J binding](http://www.slf4j.org/manual.html) is used to associate a specific logging framework with SLF4J.
* (Optional) Maven

The two dependencies, [Jackson-Core](https://github.com/FasterXML/jackson-core) and [SLF4J](http://www.slf4j.org/), will be added automatically if Maven is used. Otherwise, please download the jars and add them to your build path. 

SLF4J is only needed if you enable logging through the OperationContext class. If you plan to use logging, please also download an [SLF4J binding](http://repo2.maven.org/maven2/org/slf4j/) which will link the SLF4J API with the logging implementation of your choice. Simple is a good default. See the [SLF4J user manual](http://www.slf4j.org/manual.html) for more information.

## Usage

To use this SDK to call Microsoft Azure storage services, you need to first [create an account](https://azure.microsoft.com/free).

Samples are provided in the microsoft-azure-storage-samples folder. The unit tests in microsoft-azure-storage-test can also be helpful.

## Code Sample

The following is a quick example on how to upload a file to azure blob and download it back. You may also download and view the samples in the microsoft-azure-storage-samples folder. For additional information on using the client libraries to access Azure services see the How To guides for [blobs](http://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-blob-storage/), [queues](http://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-queue-storage/), [tables](http://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-table-storage/) and the [general documentation](http://azure.microsoft.com/en-us/develop/java/).

```java
import java.io.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

public class BlobSample {
	public static final String storageConnectionString =
		"DefaultEndpointsProtocol=http;"
		+ "AccountName=your_account_name;"
		+ "AccountKey= your_account_key";

	public static void main(String[] args) {
		try {
			CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
            CloudBlobClient serviceClient = account.createCloudBlobClient();

            // Container name must be lower case.
            CloudBlobContainer container = serviceClient.getContainerReference("myimages");
            container.createIfNotExists();

            // Upload an image file.
            CloudBlockBlob blob = container.getBlockBlobReference("image1.jpg");
            File sourceFile = new File("c:\\myimages\\image1.jpg");
            blob.upload(new FileInputStream(sourceFile), sourceFile.length());

            // Download the image file.
            File destinationFile = new File(sourceFile.getParentFile(), "image1Download.tmp");
            blob.downloadToFile(destinationFile.getAbsolutePath());
        }
        catch (FileNotFoundException fileNotFoundException) {
            System.out.print("FileNotFoundException encountered: ");
            System.out.println(fileNotFoundException.getMessage());
            System.exit(-1);
        }
        catch (StorageException storageException) {
            System.out.print("StorageException encountered: ");
            System.out.println(storageException.getMessage());
            System.exit(-1);
        }
        catch (Exception e) {
            System.out.print("Exception encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
	}
}
```

# Need Help?

Be sure to check out the Microsoft Azure [Developer Forums on MSDN](http://social.msdn.microsoft.com/Forums/windowsazure/en-US/home?forum=windowsazuredata) or the [Developer Forums on Stack Overflow](http://stackoverflow.com/questions/tagged/azure+windows-azure-storage) if you have trouble with the provided code.

# Contribute Code or Provide Feedback

If you would like to become an active contributor to this project please follow the instructions provided in [Azure Projects Contribution Guidelines](http://azure.github.io/guidelines/).

If you encounter any bugs with the library please file an issue in the [Issues](https://github.com/Azure/azure-storage-java/issues) section of the project.

When sending pull requests, please send non-breaking PRs to the dev branch and breaking changes to the dev_breaking branch. Do not make PRs against master.

# Learn More

* [Java on Azure Developer Center](http://azure.microsoft.com/en-us/java/azure)
* [Azure Storage Service](http://azure.microsoft.com/en-us/documentation/services/storage/)
* [Azure Storage Team Blog](http://blogs.msdn.com/b/windowsazurestorage/)
* [Java client library reference for Azure Storage](https://docs.microsoft.com/en-us/java/api/overview/azure/storage)
