using Microsoft.Extensions.Logging;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

namespace meterEmul;

public class ModbusMaster : IDisposable
{
    private readonly int port;
    private readonly ILogger log;

    private readonly IPAddress ipAddress;

    public ModbusMaster(int port, IPAddress ipAddress, ILogger log)
    {
        this.port = port;
        this.ipAddress = ipAddress;
        this.log = log;
    }

    private TcpClient client;

    public async Task Start(CancellationToken cancel)
    {
        client = new TcpClient();

        while (true)
        {
            try
            {
                await client.ConnectAsync(new IPEndPoint(ipAddress, port), cancel);
                return;
            }
            catch (SocketException ex)
            {
                log.LogWarning(ex, "Connect failed. Retrying");
                await Task.Delay(TimeSpan.FromSeconds(5), cancel);
            }
        }
    }

    const byte ModbusFcReadMultipleRegisters = 0x03;
    private const byte ModbusFcWriteMultipleRegisters = 16;

    private int transactionId = 0;

    public async Task<IEnumerable<ushort>> ReadMultipleRegisters(ushort addr, ushort num)
    {
        var aduFrame = new byte[12];

        BinaryPrimitives.WriteUInt16BigEndian(aduFrame, (ushort)transactionId);
        transactionId++;

        // protocol identifier
        BinaryPrimitives.WriteUInt16BigEndian(aduFrame.AsSpan(2), 0);

        // message length, 6 bytes to follow
        BinaryPrimitives.WriteUInt16BigEndian(aduFrame.AsSpan(4), 6);

        byte unit = 1;//0xC8;
        aduFrame[6] = unit;

        aduFrame[7] = ModbusFcReadMultipleRegisters;

        BinaryPrimitives.WriteUInt16BigEndian(aduFrame.AsSpan(8), addr);
        BinaryPrimitives.WriteUInt16BigEndian(aduFrame.AsSpan(10), num);

        var stream = client.GetStream();

        stream.Write(aduFrame);

        log.LogTrace($"ReadMultipleRegisters {addr} {num}");
        log.LogTrace(string.Join(" ", aduFrame.Select(b => b.ToString("x2"))));

        await stream.ReadExactlyAsync(aduFrame, 0, 7);

        var messageLen =
            BinaryPrimitives.ReadUInt16BigEndian(aduFrame.AsSpan(4));

        var pdu = new byte[messageLen-1];

        await stream.ReadExactlyAsync(pdu);

        var responseFunctionCode = pdu[0];
        if ((0x80 & responseFunctionCode) > 0)
        {
            Console.WriteLine($"Read error {responseFunctionCode:X} {pdu[1]:X}");
            Console.WriteLine($"Readind from {addr} {num}");
        }

        var receivedLen = pdu[1];

        var result = new ushort[receivedLen / 2];

        for (int i = 0; i < result.Length; i++)
        {
            result[i] = BinaryPrimitives.ReadUInt16BigEndian(pdu.AsSpan(2 + i*2));
        }

        return result;
    }

    public async Task<int> WriteMultipleRegisters(ushort addr, params ushort[] regs)
    {
        log.LogDebug($"Write {addr} {regs[0]}");

        var fullFrame = new byte[6*2 + regs.Length * 2 + 1];

        BinaryPrimitives.WriteUInt16BigEndian(fullFrame.AsSpan(), (ushort)transactionId++);
        BinaryPrimitives.WriteUInt16BigEndian(fullFrame.AsSpan(2), 0);
        BinaryPrimitives.WriteUInt16BigEndian(fullFrame.AsSpan(4), (ushort)(6 + 2 * regs.Length + 1));

        // unit identifier
        fullFrame[6] = 1;
        // function code
        fullFrame[7] = ModbusFcWriteMultipleRegisters;

        // address
        BinaryPrimitives.WriteUInt16BigEndian(fullFrame.AsSpan(8), addr);
        // number of regs
        BinaryPrimitives.WriteUInt16BigEndian(fullFrame.AsSpan(10), (ushort)regs.Length);

        fullFrame[12] = (byte)(2 * regs.Length);

        for (int i = 0; i < regs.Length; i++)
        {
            BinaryPrimitives.WriteUInt16BigEndian(fullFrame.AsSpan(13 + i), regs[i]);
        }

        var stream = client.GetStream();
        stream.Write(fullFrame);

        await stream.ReadExactlyAsync(fullFrame, 0, 9);

        var responseFunctionCode = fullFrame[7];

        if ((0x80 & responseFunctionCode) > 0)
        {
            Console.WriteLine($"Error {fullFrame[8]}");
            return 0;
        }

        await stream.ReadExactlyAsync(fullFrame, 9, 3);

        var written = BinaryPrimitives.ReadUInt16BigEndian(fullFrame.AsSpan(10));

        log.LogDebug($"Written {written}");
        return written;
    }

    public void Dispose()
    {
        client.Dispose();
    }
}
