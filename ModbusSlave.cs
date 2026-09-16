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

    private readonly ConcurrentDictionary<ushort, ushort>
        holdingRegisterValues = new();

    private const byte FcReadHoldingRegisters = 0x03;
    private const byte FcReadInputRegisters = 0x04;

    private const byte ExceptionIllegalFunction = 0x01;
    private const byte ExceptionIllegalDataAddress = 0x02;
    private const byte ExceptionIllegalDataValue = 0x03;

    public ModbusSlave(
        int port,
        string? ipAddress,
        ILogger log)
    {
        this.log = log;
        this.port = port;

        this.ipAddress = !string.IsNullOrEmpty(ipAddress)
            ? IPAddress.Parse(ipAddress)
            : IPAddress.Any;
    }

    /// <summary>
    /// Add a register that Fronius may probe during meter discovery.
    /// </summary>
    public void AddProbeRegister(ushort address, ushort value = 0)
    {
        holdingRegisterValues[address] = value;
    }

    /// <summary>
    /// Fill a range with zeroes, without overwriting values already present.
    /// </summary>
    public void EnsureRegisterRange(ushort start, ushort end)
    {
        for (int address = start; address <= end; address++)
        {
            holdingRegisterValues.TryAdd((ushort)address, 0);
        }
    }

    public Task<ushort> SetHoldingRegister(
        ushort addr,
        ushort value)
    {
        holdingRegisterValues[addr] = value;

        return Task.FromResult((ushort)(addr + 1));
    }

    public Task<ushort> SetHoldingRegister(
        ushort addr,
        float value)
    {
        var bits = BitConverter.SingleToUInt32Bits(value);

        // SunSpec FLOAT32:
        // high word first, low word second.
        holdingRegisterValues[addr] =
            (ushort)(bits >> 16);

        holdingRegisterValues[(ushort)(addr + 1)] =
            (ushort)(bits & 0xFFFF);

        return Task.FromResult((ushort)(addr + 2));
    }

    public Task<ushort> SetHoldingRegister(
        ushort addr,
        string value,
        int words)
    {
        var maxBytes = words * 2;
        var stringBytes = Encoding.UTF8.GetBytes(value);

        if (stringBytes.Length > maxBytes)
        {
            stringBytes = stringBytes
                .Take(maxBytes)
                .ToArray();
        }

        var bytes = new byte[maxBytes];

        Array.Copy(
            stringBytes,
            bytes,
            stringBytes.Length);

        for (int i = 0; i < words; i++)
        {
            holdingRegisterValues[(ushort)(addr + i)] =
                BinaryPrimitives.ReadUInt16BigEndian(
                    bytes.AsSpan(i * 2, 2));
        }

        return Task.FromResult((ushort)(addr + words));
    }

    public async Task Run(CancellationToken cancel)
    {
        var ipEndPoint =
            new IPEndPoint(ipAddress, port);

        var listener =
            new TcpListener(ipEndPoint);

        var clients =
            new List<Task>();

        listener.Start();

        log.LogInformation(
            "Modbus TCP listening on {Address}:{Port}",
            ipAddress,
            port);

        try
        {
            var acceptTask =
                listener.AcceptTcpClientAsync(cancel);

            while (!cancel.IsCancellationRequested)
            {
                var allTasks =
                    clients
                        .Concat(new[] { acceptTask.AsTask() })
                        .ToArray();

                var completedTask =
                    await Task.WhenAny(allTasks);

                if (acceptTask.IsCompleted)
                {
                    var newClient =
                        await acceptTask;

                    log.LogInformation(
                        "New Modbus client from {Remote}",
                        newClient.Client.RemoteEndPoint);

                    clients.Add(
                        ClientTask(
                            newClient,
                            cancel));

                    acceptTask =
                        listener.AcceptTcpClientAsync(cancel);
                }
                else
                {
                    try
                    {
                        await completedTask;
                    }
                    catch (OperationCanceledException)
                        when (cancel.IsCancellationRequested)
                    {
                    }
                    catch (Exception ex)
                    {
                        log.LogWarning(
                            ex,
                            "Modbus client task terminated with an error");
                    }

                    clients.Remove(completedTask);
                }
            }
        }
        finally
        {
            listener.Stop();
        }
    }

    private async Task ClientTask(
        TcpClient client,
        CancellationToken cancel)
    {
        var remote =
            client.Client.RemoteEndPoint?.ToString()
            ?? "unknown";

        try
        {
            using (client)
            {
                var stream =
                    client.GetStream();

                while (!cancel.IsCancellationRequested)
                {
                    var mbap =
                        new byte[7];

                    try
                    {
                        await stream.ReadExactlyAsync(
                            mbap,
                            cancel);
                    }
                    catch (EndOfStreamException)
                    {
                        break;
                    }

                    var transactionId =
                        BinaryPrimitives.ReadUInt16BigEndian(
                            mbap.AsSpan(0, 2));

                    var protocolId =
                        BinaryPrimitives.ReadUInt16BigEndian(
                            mbap.AsSpan(2, 2));

                    var length =
                        BinaryPrimitives.ReadUInt16BigEndian(
                            mbap.AsSpan(4, 2));

                    var unitId =
                        mbap[6];

                    if (protocolId != 0)
                    {
                        log.LogWarning(
                            "Invalid Modbus protocol ID from {Remote}: {Protocol}",
                            remote,
                            protocolId);

                        break;
                    }

                    if (length < 2)
                    {
                        log.LogWarning(
                            "Invalid Modbus length from {Remote}: {Length}",
                            remote,
                            length);

                        break;
                    }

                    var pduLength =
                        length - 1;

                    var pdu =
                        new byte[pduLength];

                    await stream.ReadExactlyAsync(
                        pdu,
                        cancel);

                    var function =
                        pdu[0];

                    log.LogInformation(
                        "MODBUS RX {Remote}: TX={Transaction} Unit={Unit} " +
                        "FC=0x{Function:X2} PDU={Pdu}",
                        remote,
                        transactionId,
                        unitId,
                        function,
                        Convert.ToHexString(pdu));

                    switch (function)
                    {
                        case FcReadHoldingRegisters:
                        case FcReadInputRegisters:

                            await HandleReadRegisters(
                                stream,
                                remote,
                                transactionId,
                                unitId,
                                function,
                                pdu,
                                cancel);

                            break;

                        default:

                            log.LogWarning(
                                "Unsupported Modbus function from {Remote}: " +
                                "TX={Transaction} Unit={Unit} FC=0x{Function:X2}",
                                remote,
                                transactionId,
                                unitId,
                                function);

                            await SendExceptionResponse(
                                stream,
                                transactionId,
                                unitId,
                                function,
                                ExceptionIllegalFunction,
                                cancel);

                            break;
                    }
                }
            }
        }
        catch (OperationCanceledException)
            when (cancel.IsCancellationRequested)
        {
        }
        catch (IOException ex)
        {
            log.LogInformation(
                "Modbus connection {Remote} closed: {Message}",
                remote,
                ex.Message);
        }
        catch (SocketException ex)
        {
            log.LogInformation(
                "Modbus socket {Remote} closed: {Message}",
                remote,
                ex.Message);
        }
        catch (Exception ex)
        {
            log.LogWarning(
                ex,
                "Unexpected error handling Modbus client {Remote}",
                remote);
        }
        finally
        {
            log.LogInformation(
                "Modbus client {Remote} connection ended",
                remote);
        }
    }

    private async Task HandleReadRegisters(
        NetworkStream stream,
        string remote,
        ushort transactionId,
        byte unitId,
        byte function,
        byte[] pdu,
        CancellationToken cancel)
    {
        if (pdu.Length != 5)
        {
            await SendExceptionResponse(
                stream,
                transactionId,
                unitId,
                function,
                ExceptionIllegalDataValue,
                cancel);

            return;
        }

        var address =
            BinaryPrimitives.ReadUInt16BigEndian(
                pdu.AsSpan(1, 2));

        var count =
            BinaryPrimitives.ReadUInt16BigEndian(
                pdu.AsSpan(3, 2));

        log.LogInformation(
            "FC{Function:X2} {Remote}: TX={Transaction} Unit={Unit} " +
            "Addr={Address} Count={Count} End={End}",
            function,
            remote,
            transactionId,
            unitId,
            address,
            count,
            count > 0
                ? (int)address + count - 1
                : address);

        if (count == 0 || count > 125)
        {
            await SendExceptionResponse(
                stream,
                transactionId,
                unitId,
                function,
                ExceptionIllegalDataValue,
                cancel);

            return;
        }

        if ((int)address + count > 65536)
        {
            await SendExceptionResponse(
                stream,
                transactionId,
                unitId,
                function,
                ExceptionIllegalDataAddress,
                cancel);

            return;
        }

        /*
         * Match the reference emulator:
         *
         * every requested register must exist.
         * Unknown addresses produce Modbus exception 02.
         */
        for (int i = 0; i < count; i++)
        {
            var registerAddress =
                (ushort)(address + i);

            if (!holdingRegisterValues.ContainsKey(registerAddress))
            {
                log.LogWarning(
                    "Unknown register requested by {Remote}: " +
                    "Unit={Unit} FC=0x{Function:X2} Register={Register}",
                    remote,
                    unitId,
                    function,
                    registerAddress);

                await SendExceptionResponse(
                    stream,
                    transactionId,
                    unitId,
                    function,
                    ExceptionIllegalDataAddress,
                    cancel);

                return;
            }
        }

        var responsePdu =
            new byte[2 + count * 2];

        responsePdu[0] =
            function;

        responsePdu[1] =
            (byte)(count * 2);

        for (int i = 0; i < count; i++)
        {
            var registerAddress =
                (ushort)(address + i);

            var value =
                holdingRegisterValues[registerAddress];

            BinaryPrimitives.WriteUInt16BigEndian(
                responsePdu.AsSpan(
                    2 + i * 2,
                    2),
                value);
        }

        await SendResponse(
            stream,
            transactionId,
            unitId,
            responsePdu,
            cancel);

        log.LogInformation(
            "MODBUS TX {Remote}: TX={Transaction} Unit={Unit} " +
            "FC=0x{Function:X2} Addr={Address} Count={Count} OK",
            remote,
            transactionId,
            unitId,
            function,
            address,
            count);
    }

    private async Task SendExceptionResponse(
        NetworkStream stream,
        ushort transactionId,
        byte unitId,
        byte function,
        byte exceptionCode,
        CancellationToken cancel)
    {
        var responsePdu =
            new byte[]
            {
                (byte)(function | 0x80),
                exceptionCode
            };

        await SendResponse(
            stream,
            transactionId,
            unitId,
            responsePdu,
            cancel);

        log.LogWarning(
            "MODBUS TX EXCEPTION: TX={Transaction} Unit={Unit} " +
            "FC=0x{Function:X2} Exception=0x{Exception:X2}",
            transactionId,
            unitId,
            function,
            exceptionCode);
    }

    private static async Task SendResponse(
        NetworkStream stream,
        ushort transactionId,
        byte unitId,
        byte[] pdu,
        CancellationToken cancel)
    {
        var response =
            new byte[7 + pdu.Length];

        BinaryPrimitives.WriteUInt16BigEndian(
            response.AsSpan(0, 2),
            transactionId);

        BinaryPrimitives.WriteUInt16BigEndian(
            response.AsSpan(2, 2),
            0);

        BinaryPrimitives.WriteUInt16BigEndian(
            response.AsSpan(4, 2),
            (ushort)(1 + pdu.Length));

        response[6] =
            unitId;

        pdu.CopyTo(
            response.AsSpan(7));

        await stream.WriteAsync(
            response,
            cancel);
    }
}