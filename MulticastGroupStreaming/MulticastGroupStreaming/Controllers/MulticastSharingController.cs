using Microsoft.AspNetCore.Components.Forms;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MulticastGroupStreaming.Services;
using System.Diagnostics;
using System.IO;
using System.IO.Hashing;
using System.Text;
namespace MulticastGroupStreaming.Controllers
{

    [Route("api/[controller]/[action]")]
    [ApiController]
    public class MulticastSharingController(SendService service) : ControllerBase
    {

        static readonly int _dataSize = SendService.PayloadSize - 2 * sizeof(int);

        private static readonly string _localfilesFolder = Environment.CurrentDirectory + "\\LocalData";

        int CalculateFileIndex(List<string> fileList, long startOffset)
        {
            int fileIndex = 0;
            foreach (var file in fileList)
            {
                var fileInfo = new FileInfo($"{service.WorkingDirectory}\\{file}");
                long fileHashClusterCount = (long)Math.Ceiling(fileInfo.Length / (decimal)_dataSize);
                if (startOffset <= fileHashClusterCount)
                    break;
                else startOffset -= fileHashClusterCount;
                fileIndex++;
            }
            return fileIndex;
        }

        long CalculateOffsetInFile(List<string> fileList, long startOffset)
        {
            foreach (var file in fileList)
            {
                var fileInfo = new FileInfo($"{service.WorkingDirectory}\\{file}");
                long fileHashClusterCount = (long)Math.Ceiling(fileInfo.Length / (decimal)_dataSize);
                if (startOffset > fileHashClusterCount)
                    startOffset -= fileHashClusterCount;
                else return startOffset * _dataSize;
            }
            return startOffset * _dataSize;
        }

        [HttpGet]
        [LocalIp]
        public IActionResult GetFolderHashSum([FromServices] ILogger<MulticastSharingController> logger)
        {
            Directory.CreateDirectory(_localfilesFolder);
            try
            {
                var fileList = GetAllFilesInFolder(service.WorkingDirectory, service.WorkingDirectory);
                var filePart = new byte[_dataSize];
                var hashFile = _localfilesFolder + "\\hashes";
                fileList.Remove(hashFile);

                using (FileStream hashSums = System.IO.File.Create(hashFile))
                {
                    foreach (string file in fileList)
                    {
                        logger.LogInformation($"Calculate {file}");
                        using var reader = new FileStream($"{service.WorkingDirectory}\\{file}", FileMode.Open, FileAccess.Read);
                        int bytesRead;
                        do
                        {
                            bytesRead = reader.Read(filePart, 0, _dataSize);
                            hashSums.Write(XxHash32.Hash(filePart[..bytesRead]));
                        } while (bytesRead == _dataSize);
                    }
                }
                logger.LogInformation("Контрольные суммы посчитаны успешно");
            }
            catch (DirectoryNotFoundException ex)
            {
                logger.LogError("{Message}", ex.Message);
                return StatusCode(400);
            }
            catch
            {
                logger.LogError("Произошла неизвестная ошибка при вычислении контрольных сумм");
                return StatusCode(400);
            }
            return StatusCode(200);
        }

        [HttpGet]
        public PhysicalFileResult DownloadHashSums()
        {
            return PhysicalFile(_localfilesFolder + "\\hashes", "application/octet-stream", "Put it in client localdata folder");
        }

        private static bool isSending = false;

        private static readonly HashSet<int> _startOffsets = [];

        private static readonly HashSet<int> _endOffsets = [];

        private static int _lastDownloadStartFileOffset = 0;

        private static int _lastDownloadEndFileOffset = 0;

        private static int _lastDownloadStartFileIndex = 0;

        private static int _lastDownloadEndFileIndex = 0;

        private static long _lastDownloadOffsetInFile = 0;

        private static List<string> files = new List<string>();

        [HttpGet]
        public Tuple<long, int, int> GetOffsetsForDownloading(int startOffset = 0, int endOffset = 0)
        {
            if (_startOffsets.Count > 10)
            {
                var lastValue = _startOffsets.Min();
                _startOffsets.Clear();
                _startOffsets.Add(lastValue);
                lastValue = _endOffsets.Min();
                _endOffsets.Clear();
                _endOffsets.Add(lastValue);
            }
            _startOffsets.Add(startOffset);
            _endOffsets.Add(endOffset);

            if (isSending) 
                return (_lastDownloadOffsetInFile, _lastDownloadStartFileIndex, _lastDownloadEndFileIndex).ToTuple();

            files = GetAllFilesInFolder(service.WorkingDirectory, service.WorkingDirectory);

            if (endOffset <= 0)
            {
                int offset = 0;

                foreach (var file in files)
                {
                    var fileInfo = new FileInfo($"{service.WorkingDirectory}\\{file}");
                    offset += (int)Math.Ceiling(fileInfo.Length / (decimal)_dataSize);

                }
                _lastDownloadEndFileOffset = offset;
            }
            else
                _lastDownloadEndFileOffset = _endOffsets.Max();

            _lastDownloadStartFileOffset = _startOffsets.Min();

            _lastDownloadStartFileIndex = CalculateFileIndex(files, _lastDownloadStartFileOffset);

            _lastDownloadEndFileIndex = CalculateFileIndex(files, _lastDownloadEndFileOffset);

            _lastDownloadOffsetInFile = CalculateOffsetInFile(files, _lastDownloadStartFileOffset);

            return (_lastDownloadOffsetInFile, _lastDownloadStartFileIndex, _lastDownloadEndFileIndex).ToTuple();
        }

        [HttpGet]
        public IActionResult StartSending()
        {
            if (isSending)
                return StatusCode(102);
#pragma warning disable CS4014 // Так как этот вызов не ожидается, выполнение существующего метода продолжается до тех пор, пока вызов не будет завершен
            var sendingThread = new Thread(() => SendData(files, _lastDownloadStartFileOffset, _lastDownloadEndFileOffset));
            sendingThread.Start();
#pragma warning restore CS4014 // Так как этот вызов не ожидается, выполнение существующего метода продолжается до тех пор, пока вызов не будет завершен
            return StatusCode(200);
        }

        private async Task SendData(List<string> fileList, int startOffsetInBlocks, int endOffsetInBlocks)
        {
            var wait = Task.Delay(500);
            object locker = new();
            isSending = true;
            var filePart = new byte[SendService.PayloadSize];
            await wait;
            int startFile = CalculateFileIndex(fileList, startOffsetInBlocks);
            int endFile = CalculateFileIndex(fileList, endOffsetInBlocks);
            var startOffsetInFile = CalculateOffsetInFile(fileList, startOffsetInBlocks);
            var globalStartOffset = startOffsetInBlocks;
            var localStartOffset = (int)(startOffsetInFile / _dataSize);
            for (int index = startFile; index <= endFile; index++)
            {
                using var reader = new FileStream($"{service.WorkingDirectory}\\{fileList[index]}", FileMode.Open, FileAccess.Read, FileShare.Read, _dataSize * 5);
                int bytesRead;
                reader.Position = startOffsetInFile;
                do
                {
                    if (globalStartOffset++ == endOffsetInBlocks)
                        break;
                    bytesRead = await reader.ReadAsync(filePart, 0, _dataSize);
                    var datagram = AssembleDatagram(filePart, bytesRead, index, localStartOffset++);
                    await service.UdpSender.SendAsync(datagram, datagram.Length, service.GroupInfo);

                } while (bytesRead == _dataSize);
                await service.UdpSender.SendAsync(Encoding.UTF8.GetBytes("NEXT"), 4, service.GroupInfo);
                localStartOffset = 0;
                startOffsetInFile = 0;
            }
            Debug.WriteLine("end");
            isSending = false;
            lock (locker)
            {
                _startOffsets.Remove(startOffsetInBlocks);
                _endOffsets.Remove(endOffsetInBlocks);
            }
        }

        private static byte[] AssembleDatagram(byte[] blockOfData, int actualDataSize, int indexOfFile, int blockOffset)
        {
            var assembledDatagram = new byte[actualDataSize + 2 * sizeof(int)];
            var indexInBytes = BitConverter.GetBytes(indexOfFile);
            var offsetInBytes = BitConverter.GetBytes(blockOffset);
            Array.Copy(blockOfData, assembledDatagram, actualDataSize);
            Array.Copy(indexInBytes, 0, assembledDatagram, actualDataSize, indexInBytes.Length);
            Array.Copy(offsetInBytes, 0, assembledDatagram, actualDataSize + indexInBytes.Length, offsetInBytes.Length);

            return assembledDatagram;
        }

        private static List<string> GetAllFilesInFolder(string srcFolder, string destFolder)
        {
            var filesInDirectory = Directory.GetFiles(destFolder).Select(e => Path.GetRelativePath(srcFolder, e)).ToList();
            var foldersInDirectory = Directory.GetDirectories(destFolder).Select(e => Path.GetRelativePath(srcFolder, e)).ToList();
            List<string> filesInSubdirectory;
            foreach (var item in foldersInDirectory)
            {
                filesInSubdirectory = GetAllFilesInFolder(srcFolder, $"{srcFolder}\\{item}");
                foreach (var file in filesInSubdirectory)
                    filesInDirectory.Add(file);
            }
            return filesInDirectory;
        }

        [HttpGet]
        public List<Tuple<string, long>> GetFileNames()
        {
            var fileNames = GetAllFilesInFolder(service.WorkingDirectory, service.WorkingDirectory);
            var fileInfo = new List<Tuple<string, long>>();
            foreach (var name in fileNames)
            {
                FileInfo file = new($"{service.WorkingDirectory}\\{name}");
                fileInfo.Add((name, file.Length).ToTuple());
            }
            return fileInfo;
        }

        [HttpGet]
        public Tuple<int, int> GetPacketAndSendBufferSize()
        {
            return (SendService.PayloadSize, service.UdpSender.Client.SendBufferSize).ToTuple();
        }
    }
}
