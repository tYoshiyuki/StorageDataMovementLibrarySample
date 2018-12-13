using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.DataMovement;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace StorageDataMovementLibrarySample
{
    public class Program
    {
        // MsDoc参照
        // https://docs.microsoft.com/ja-jp/azure/storage/common/storage-use-data-movement-library
        public static void Main(string[] args)
        {
            Console.WriteLine("Enter Storage account name:");
            var accountName = Console.ReadLine();

            Console.WriteLine("\nEnter Storage account key:");
            var accountKey = Console.ReadLine();

            var storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=" + accountName + ";AccountKey=" + accountKey;
            var account = CloudStorageAccount.Parse(storageConnectionString);

            ExecuteChoice(account);
        }

        private static void ExecuteChoice(CloudStorageAccount account)
        {
            Console.WriteLine("\nWhat type of transfer would you like to execute?" +
                "\n1. Local file --> Azure Blob" +
                "\n2. Local directory --> Azure Blob directory" +
                "\n3. URL (e.g. Amazon S3 file) --> Azure Blob" +
                "\n4. Azure Blob --> Azure Blob");
            var choice = int.Parse(Console.ReadLine());

            // 並行処理数の設定
            Console.WriteLine("\nHow many parallel operations would you like to use?");
            TransferManager.Configurations.ParallelOperations = int.Parse(Console.ReadLine());

            // 処理の分岐
            switch (choice)
            {
                case 1:
                    TransferLocalFileToAzureBlob(account).Wait();
                    break;

                case 2:
                    TransferLocalDirectoryToAzureBlobDirectory(account).Wait();
                    break;

                case 3:
                    TransferUrlToAzureBlob(account).Wait();
                    break;

                case 4:
                    TransferAzureBlobToAzureBlob(account).Wait();
                    break;
            }

            // 再度、処理内容を選択させる
            ExecuteChoice(account);
        }

        private static async Task TransferLocalFileToAzureBlob(CloudStorageAccount account)
        {
            // From・Toの情報を取得
            var localFilePath = GetSourcePath();
            var blob = GetBlob(account);

            // 転送処理の実施
            Console.WriteLine("\nTransfer started...\n");
            var stopWatch = Stopwatch.StartNew();
            await TransferManager.UploadAsync(localFilePath, blob, null, GetSingleTransferContext());
            stopWatch.Stop();
            Console.WriteLine("\nTransfer operation completed in " + stopWatch.Elapsed.TotalSeconds + " seconds.");
        }

        private static async Task TransferLocalDirectoryToAzureBlobDirectory(CloudStorageAccount account)
        {
            // From・Toの情報を取得
            var localDirectoryPath = GetSourcePath();
            var blobDirectory = GetBlobDirectory(account);

            // 再帰的にアップロードを実施する
            var options = new UploadDirectoryOptions()
            {
                Recursive = true
            };

            // 転送処理の実施
            Console.WriteLine("\nTransfer started...\n");
            var stopWatch = Stopwatch.StartNew();
            await TransferManager.UploadDirectoryAsync(localDirectoryPath, blobDirectory, options, GetDirectoryTransferContext());
            stopWatch.Stop();
            Console.WriteLine("\nTransfer operation completed in " + stopWatch.Elapsed.TotalSeconds + " seconds.");
        }

        private static async Task TransferUrlToAzureBlob(CloudStorageAccount account)
        {
            // From・Toの情報を取得
            var uri = new Uri(GetSourcePath());
            var blob = GetBlob(account);

            // 転送処理の実施
            Console.WriteLine("\nTransfer started...\n");
            var stopWatch = Stopwatch.StartNew();
            await TransferManager.CopyAsync(uri, blob, true, null, GetSingleTransferContext());
            stopWatch.Stop();
            Console.WriteLine("\nTransfer operation completed in " + stopWatch.Elapsed.TotalSeconds + " seconds.");
        }

        private static async Task TransferAzureBlobToAzureBlob(CloudStorageAccount account)
        {
            // From・Toの情報を取得
            CloudBlockBlob sourceBlob = GetBlob(account);
            CloudBlockBlob destinationBlob = GetBlob(account);

            // 転送処理の実施
            Console.WriteLine("\nTransfer started...\n");
            var stopWatch = Stopwatch.StartNew();
            await TransferManager.CopyAsync(sourceBlob, destinationBlob, true, null, GetSingleTransferContext());
            stopWatch.Stop();
            Console.WriteLine("\nTransfer operation completed in " + stopWatch.Elapsed.TotalSeconds + " seconds.");
        }

        private static string GetSourcePath()
        {
            Console.WriteLine("\nProvide path for source:");
            return Console.ReadLine();
        }

        private static CloudBlockBlob GetBlob(CloudStorageAccount account)
        {
            var blobClient = account.CreateCloudBlobClient();

            Console.WriteLine("\nProvide name of Blob container:");
            var containerName = Console.ReadLine();

            // コンテナの取得
            var container = blobClient.GetContainerReference(containerName);
            container.CreateIfNotExistsAsync().Wait();

            Console.WriteLine("\nProvide name of new Blob:");
            var blobName = Console.ReadLine();

            // Blobの取得
            var blob = container.GetBlockBlobReference(blobName);

            return blob;
        }

        private static SingleTransferContext GetSingleTransferContext()
        {
            return new SingleTransferContext
            {
                ProgressHandler = new Progress<TransferStatus>(progress => Console.Write($"\rBytes transferred: {progress.BytesTransferred}"))
            };
        }

        private static DirectoryTransferContext GetDirectoryTransferContext()
        {
            return new DirectoryTransferContext
            {
                ProgressHandler = new Progress<TransferStatus>(progress => Console.Write($"\rBytes transferred: {progress.BytesTransferred}"))
            };
        }

        private static CloudBlobDirectory GetBlobDirectory(CloudStorageAccount account)
        {
            var blobClient = account.CreateCloudBlobClient();

            Console.WriteLine("\nProvide name of Blob container. This can be a new or existing Blob container:");
            var containerName = Console.ReadLine();

            // コンテナの取得
            var container = blobClient.GetContainerReference(containerName);
            container.CreateIfNotExistsAsync().Wait();

            // Blobディレクトリの取得
            var blobDirectory = container.GetDirectoryReference("");

            return blobDirectory;
        }
    }
}
