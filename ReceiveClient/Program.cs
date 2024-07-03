using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.Hashing;
using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Reflection.Metadata;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Configuration;
using static System.Net.Mime.MediaTypeNames;

#pragma warning disable CS8604 // Возможно, аргумент-ссылка, допускающий значение NULL.
namespace ReceiveClient
{
    public class App
    {
        public const byte PARALLELISMDEGREE = 2;

        public IConfigurationRoot Config { get; }

        public const string LocalFiles = "LocalData";

        public App()
        {
            var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("C:\\Users\\Практика\\source\\repos\\ReceiveClient\\appsettings.json");
            Config = builder.Build();
            Directory.CreateDirectory(LocalFiles);
        }
    }
    public class UdpReceiver
    {
        class Datagram
        {
            public byte[] Data { get; set; }
            public int IndexOfFile { get; }
            public int BlockOffset { get; }
            public Datagram(byte[] packet)
            {
                var dataLength = packet.Length - 2 * sizeof(int);
                Data = new byte[dataLength];
                Array.Copy(packet, Data, dataLength);
                IndexOfFile = BitConverter.ToInt32(new ReadOnlySpan<byte>(packet, dataLength, sizeof(int)));
                BlockOffset = BitConverter.ToInt32(new ReadOnlySpan<byte>(packet, dataLength + sizeof(int), sizeof(int)));
            }
        }
        public UdpClient Receiver { get; set; }
        readonly IConfigurationRoot _config;

        // Полезная нагрузка - размер блока данных + индекс файла + смещение блока данных в файле
        public int PayloadSize { get; set; }

        readonly ConcurrentQueue<byte[]> _queue = new();

        static int count = 0;
        static bool isCorrupted = false;
        void WriteData(FileStream[] streams, byte[] fileInfo, List<Tuple<string, long>> fileList, ActionBlock<byte[]> writerBlock)
        {
            var randomizer = new Random();
            var data = new Datagram(fileInfo);
            if (streams[data.IndexOfFile] is null)
            {
                var file = new FileStream($"{_config["Directory"]}\\{fileList[data.IndexOfFile].Item1}", FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
                file.SetLength(fileList[data.IndexOfFile].Item2);
                streams[data.IndexOfFile] = file;
            }
            if (randomizer.NextDouble() <= 0.01 && !isCorrupted)
            {
                Debug.WriteLine("Datagram was corrupted");
                data.Data[0] = 0;
                isCorrupted = true;
            }
            streams[data.IndexOfFile].Position = data.BlockOffset * (PayloadSize - 2 * sizeof(int));
            streams[data.IndexOfFile].Write(data.Data, 0, data.Data.Length);
            Console.WriteLine($"Wrote block on {data.BlockOffset}");
            Console.WriteLine($"Queue buffer: {writerBlock.InputCount} blocks");
        }
        public UdpReceiver(IConfigurationRoot Config, int payloadSize, int recBufferSize)
        {
            Receiver = new UdpClient(int.Parse(Config["Port"]));
            Receiver.JoinMulticastGroup(IPAddress.Parse(Config["MulticastGroupAddress"]));
            _config = Config;
            PayloadSize = payloadSize;
            Receiver.Client.SendBufferSize = recBufferSize;
        }

        public async Task ReadDataFromSocket(string fileName, ITargetBlock<byte[]> target,
     CancellationTokenSource cancellationToken)
        {
            var buffer = new byte[PayloadSize];
            Debug.WriteLine($"{Task.CurrentId} {fileName} started");
            while (true)
            {
                try
                {
                    ArraySegment<byte> buffer1 = new(buffer);
                    var result = await Receiver.Client.ReceiveAsync(buffer1, cancellationToken.Token);
                    var content = new byte[result];
                    Array.Copy(buffer, content, result);
                    if (content.Length != 4)
                    {
                        target.Post(content);
                    }
                    else
                        if (Encoding.UTF8.GetString(content) == "NEXT")
                    {
                        Debug.WriteLine($"{Task.CurrentId} task has completed  {fileName}");
                        cancellationToken.Cancel();
                        break;
                    }

                }
                catch (OperationCanceledException)
                {
                    Debug.WriteLine($"{Task.CurrentId} task has completed");
                    break;
                }
            }
        }

        public async Task BeginReceiving(WebClient client, List<Tuple<string, long>> fileList, Tuple<long, int, int> offsets)
        {
            long receivedOffsetInFile = offsets.Item1;
            var receivedStartFileIndex = offsets.Item2;
            var firstCorrectFileAfterCorrupted = offsets.Item3 + 1;
            var stopwatch = Stopwatch.StartNew();
            Task[] readingThreads = new Task[App.PARALLELISMDEGREE];
            int border;
            if (firstCorrectFileAfterCorrupted < 0)
                border = fileList.Count;
            else
                border = firstCorrectFileAfterCorrupted;
            FileStream[] streams = new FileStream[border];
            client.Receive();
            ActionBlock<byte[]>? writerBlock = null;
            writerBlock = new((resources) => WriteData(streams, resources, fileList, writerBlock));
            for (int i = receivedStartFileIndex; i < border; i++)
            {
                var tokenSource = new CancellationTokenSource();
                for (int index = 0; index < App.PARALLELISMDEGREE; index++)
                    readingThreads[index] = Task.Run(() => ReadDataFromSocket(fileList[i].Item1, writerBlock, tokenSource));
                await Task.WhenAll(readingThreads);
                if (receivedOffsetInFile == 0)
                    isCorrupted = false;
            }
            writerBlock.Complete();
            await writerBlock.Completion.ConfigureAwait(false);
            foreach (var stream in streams)
                stream?.Dispose();
            stopwatch.Stop();
            Console.WriteLine(stopwatch.ElapsedMilliseconds);
        }
    }
    public class WebClient(IConfigurationRoot Config, string LocalFiles)
    {
        public HttpClient HttpClient { get; } = new HttpClient();

        readonly string url = "https://" + Config["ServerAddress"] + ":8080/api/MulticastSharing/";

        readonly string localFiles = LocalFiles;

        public async Task GetHashSumsFile()
        {
            HttpResponseMessage? hashes;
            while (true)
            {
                try
                {
                    hashes = await HttpClient.SendAsync(new HttpRequestMessage
                    {
                        RequestUri = new Uri(url + "DownloadHashSums"),
                        Method = HttpMethod.Get
                    });
                    break;
                }
                catch (HttpRequestException)
                {
                    Console.WriteLine("Целевая точка отправки недоступна, попытка переподключения через 5 сек");
                    Thread.Sleep(5000);
                }
            }
            using var originalHashesFile = new FileStream($"{localFiles}\\hashes", FileMode.Create);
            await hashes.Content.CopyToAsync(originalHashesFile);
        }
        public async Task<Tuple<int, int>> GetHashSumsAndSendBufferSizes()
        {
            var result = await HttpClient.GetFromJsonAsync<Tuple<int, int>>(url + "GetPacketAndSendBufferSize");
            if (result is not null)
                return result;
            return (-1, -1).ToTuple();
        }

        public async Task<List<Tuple<string, long>>> GetFileNames()
        {
            var result = await HttpClient.GetFromJsonAsync<List<Tuple<string, long>>>(url + "GetFileNames");
            if (result is not null)
                return result;
            return [];
        }

        public async Task<Tuple<long, int, int>> GetStartOffsetsForDownloading(int startPosition = 0, int endPosition = 0)
        {
            var result = await HttpClient.GetFromJsonAsync<Tuple<long, int, int>>(url +
                $"GetOffsetsForDownloading/?startOffset={startPosition}&endOffset={endPosition}");
            if (result is not null)
                return result;
            return (-1L, -1, -1).ToTuple();
        }

        public async void Receive()
        {
            await HttpClient.GetAsync(url + "StartSending");
        }
    }
    public static class Program
    {
        static void CalculateHash(App app, List<Tuple<string, long>> fileList, int packetSize)
        {
            var filePart = new byte[packetSize];
            var hashFile = $"{App.LocalFiles}\\Hashes after downloading";
            using (FileStream hashSums = File.Create(hashFile))
                for (int i = 0; i < fileList.Count; i++)
                {
                    //Console.WriteLine($"Calculate {fileList[i].Item1}");
                    using var reader = new FileStream($"{app.Config["Directory"]}\\{fileList[i].Item1}", FileMode.Open, FileAccess.Read, FileShare.Read);
                    int bytesRead;
                    do
                    {
                        bytesRead = reader.Read(filePart, 0, packetSize);
                        hashSums.Write(XxHash32.Hash(filePart[..bytesRead]));
                    } while (bytesRead == packetSize);
                }
            Console.WriteLine("Контрольные суммы посчитаны успешно");
        }

        public static async Task Main()
        {
            var app = new App();
            var webClient = new WebClient(app.Config, App.LocalFiles);

            var buffersSize = await webClient.GetHashSumsAndSendBufferSizes();
            int payloadSize = buffersSize.Item1;

            var udpClient = new UdpReceiver(app.Config, payloadSize, buffersSize.Item2 * 8);

            var fileList = await webClient.GetFileNames();

            if (fileList is null)
                return;
            else
                foreach (var file in fileList)
                {
                    var path = Path.GetDirectoryName(file.Item1);
                    if (path != "")
                        Directory.CreateDirectory($"{app.Config["Directory"]}\\{path}");
                }
            await webClient.GetHashSumsFile();
            var result = await webClient.GetStartOffsetsForDownloading();

            while (true)
            {
                Console.WriteLine("Starting receiving");
                await udpClient.BeginReceiving(webClient, fileList, result);

                CalculateHash(app, fileList, payloadSize - 2 * sizeof(int));

                var calculatedHashFile = $"{App.LocalFiles}\\Hashes after downloading";
                var originalHashFile = $"{App.LocalFiles}\\hashes";
                if (NotEquals(calculatedHashFile, originalHashFile, out int blockStartPosition, out int blockEndPosition))
                {
                    result = await webClient.GetStartOffsetsForDownloading(blockStartPosition, blockEndPosition);
                }
                else
                    break;
            }
            udpClient.Receiver.DropMulticastGroup(IPAddress.Parse(app.Config["MulticastGroupAddress"]));
            Console.WriteLine("Передача завершена успешно");
        }
        static bool NotEquals(string hashFile1, string hashFile2, out int blockStartPosition, out int blockEndPosition)
        {
            using var originalFile = new FileStream(hashFile1, FileMode.Open);
            using var calculatedFile = new FileStream(hashFile2, FileMode.Open);
            bool isFileCorrupted = false;
            blockStartPosition = blockEndPosition = 0;

            while (Math.Max(originalFile.Position, calculatedFile.Position) < Math.Min(originalFile.Length, calculatedFile.Length))
            {
                int[] originalBlock = new int[4];
                int[] calculatedBlock = new int[4];
                for (int i = 0; i < 4; i++)
                {
                    originalBlock[i] = originalFile.ReadByte();
                    calculatedBlock[i] = calculatedFile.ReadByte();
                }
                if (!originalBlock.SequenceEqual(calculatedBlock))
                {
                    isFileCorrupted = true;
                    blockEndPosition = blockStartPosition;
                    while (!originalBlock.SequenceEqual(calculatedBlock) && !originalBlock.Contains(-1))
                    {
                        for (int i = 0; i < 4; i++)
                        {
                            originalBlock[i] = originalFile.ReadByte();
                            calculatedBlock[i] = calculatedFile.ReadByte();
                        }
                        blockEndPosition++;
                    }
                    break;
                }
                blockStartPosition++;
            }
            return isFileCorrupted;
        }
    }
}