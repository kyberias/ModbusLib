using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging;

namespace meterEmul;

public class ModbusSlave
{
    private readonly int port;
    private readonly ILogger log;

    private readonly IPAddress ipAddress;

    public ModbusSlave(int port, string? ipAddress, ILogger log)
    {
        this.log = log;
        this.port = port;

        this.ipAddress = !string.IsNullOrEmpty(ipAddress) ? IPAddress.Parse(ipAddress) : IPAddress.Any;
    }

    public async Task Run(CancellationToken cancel)
    {
        var ipEndPoint = new IPEndPoint(ipAddress, port);
        TcpListener listener = new(ipEndPoint);

        List<Task> clients = new List<Task>();

        listener.Start();
        var acceptTask = listener.AcceptTcpClientAsync(cancel);

        while (!cancel.IsCancellationRequested)
        {
            var completedTask = await Task.WhenAny(clients.Concat(new[] { acceptTask.AsTask() }));

            if (acceptTask.IsCompleted)
            {
                var newClient = await acceptTask;

                clients.Add(ClientTask(newClient, cancel));

                acceptTask = listener.AcceptTcpClientAsync(cancel);
            }
            else
            {
                try
                {
                    await completedTask;
                }
                catch (Exception ex)
                {
                    log.LogWarning(ex, ex.Message);
                }

                clients.Remove(completedTask);
            }
        }

        listener.Stop();
    }

    private const byte ModbusFcReadMultipleRegisters = 0x03;
    private const int ModbusMaxHoldingRegisters = 65536;

    private readonly ConcurrentDictionary<ushort, ushort> holdingRegisterValues = new ();

    public async Task<ushort> SetHoldingRegister(ushort addr, ushort value)
    {
        holdingRegisterValues[addr] = value;
        return (ushort)(addr + 1);
    }

    public Task<ushort> SetHoldingRegister(ushort addr, float value)
    {
        log.LogInformation($"SetHoldingRegister {addr} = {value}");

        var bits = BitConverter.SingleToUInt32Bits(value);

        holdingRegisterValues[addr] = (ushort)(bits >> 16);
        holdingRegisterValues[(ushort)(addr+1)] = (ushort)(bits & 0xFFFF);
        return Task.FromResult((ushort)(addr + 2));
    }

    public async Task<ushort> SetHoldingRegister(ushort addr, string value, int words)
    {
        var maxBytes = words * 2;
        var stringBytes = Encoding.UTF8.GetBytes(value);

        if (stringBytes.Length > maxBytes)
        {
            stringBytes = stringBytes.Take(maxBytes).ToArray();
        }

        var bytes = stringBytes.Concat(Enumerable.Range(0, maxBytes - stringBytes.Length).Select(a => (byte)0)).ToArray();

        for (int i = 0; i < words; i++)
        {
            holdingRegisterValues[(ushort)(addr + i)] = BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(i * 2));
        }

        return (ushort)(addr + words);
    }

    async Task ClientTask(TcpClient client, CancellationToken cancel)
    {
        log.LogInformation($"New client from {client.Client.RemoteEndPoint}");
        var stream = client.GetStream();

        var aduFrame = new byte[7];

        while (!cancel.IsCancellationRequested)
        {
            // Read ADU = Application Data Unit = Additional address + PDU + error check

            await stream.ReadExactlyAsync(aduFrame, cancel);

            // 0-1 Transaction ID 
            // 2-3 Protocol ID
            // 4-5 Length
            // 6 Unit ID
            // 7 Function Code
            // 8 Data length
            // 9-.. Data...

            var transactionId = BinaryPrimitives.ReadUInt16BigEndian(aduFrame);
            var protocolId = BinaryPrimitives.ReadUInt16BigEndian(aduFrame.AsSpan(2));
            var length = BinaryPrimitives.ReadUInt16BigEndian(aduFrame.AsSpan(4));
            var unitId = aduFrame[6];

            log.LogDebug($"ADU frame: {transactionId} {protocolId} {length} {unitId}");

            var data = new byte[length-1];

            await stream.ReadExactlyAsync(data, cancel);

            var function = data[0];
            log.LogDebug($"Function: {function:X}");

            switch (function)
            {
                case ModbusFcReadMultipleRegisters:
                    var addr = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(1));
                    var num = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(3));

                    log.LogDebug($"ReadMultipleRegisters addr:{addr} num:{num}");

                    BinaryPrimitives.WriteUInt16BigEndian(aduFrame, transactionId);
                    BinaryPrimitives.WriteUInt16BigEndian(aduFrame.AsSpan(2), protocolId);
                    BinaryPrimitives.WriteUInt16BigEndian(aduFrame.AsSpan(4), (ushort)(num * 2 + 3));
                    aduFrame[6] = unitId;

                    await stream.WriteAsync(aduFrame, cancel);

                    bool errorSent = false;

                    /*for (int i = 0; i < num; i++)
                    {
                        if (!holdingRegisterValues.ContainsKey((ushort)(addr + i)))
                        {
                            var errorData = new byte[2];
                            errorData[0] = (byte)(function | 0x80);
                            errorData[1] = 0x02; // Exception: illegal data address
                            await stream.WriteAsync(errorData, cancel);
                            errorSent = true;
                            break;
                        }
                    }*/

                    if (errorSent)
                    {
                        break;
                    }

                    var responseData = new byte[2 + num*2];

                    responseData[0] = function;
                    responseData[1] = (byte)(num * 2);
                    for (int i = 0; i < num; i++)
                    {
                        var regaddr = (ushort)(addr + i);
                        BinaryPrimitives.WriteUInt16BigEndian(responseData.AsSpan(2 + i*2), holdingRegisterValues.ContainsKey(regaddr) ? holdingRegisterValues[regaddr] : (ushort)0);
                    }

                    await stream.WriteAsync(responseData, cancel);

                    break;
                default:
                    log.LogWarning($"Unknown Modbus function: {function}");
                    break;
            }
        }
    }

}