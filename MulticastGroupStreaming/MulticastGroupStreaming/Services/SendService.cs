using System.Net;
using System.Net.Sockets;

namespace MulticastGroupStreaming.Services
{
    public class SendService
    {
        /// <summary>
        /// Размер блока данных + индекс файла + порядковый номер этого блока в файле
        /// </summary>
        public const int PayloadSize = 1024 * 63 + 2 * sizeof(int);
        public IPEndPoint GroupInfo { get; }
        public UdpClient UdpSender { get; set; }
        public string WorkingDirectory { get; }
        public SendService(string directory, string ip, int port)
        {
            GroupInfo = new IPEndPoint(IPAddress.Parse(ip), port);
            UdpSender = new UdpClient();
            UdpSender.Client.SendBufferSize = 1024 * 1024;
            WorkingDirectory = directory;
        }
    }
}
