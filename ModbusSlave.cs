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

        this.ipAddress = !string.IsNullOrEmpty(ipAddress)
            ? IPAddress.Parse(ipAddress)
            : IPAddress.Any;
    }

    public async Task Run(CancellationToken cancel)
    {
        var ipEndPoint = new IPEndPoint(ipAddress, port);
        TcpListener listener = new(ipEndPoint);

        List<Task> clients = new();

        listener.Start();

        log.LogInformation(
            "Modbus TCP listening on {Address}:{Port}",
            ipAddress,
            port);

        var acceptTask = listener.AcceptTcpClientAsync(cancel);

        try
        {
            while (!cancel.IsCancellationRequested)
            {
                var completedTask =
                    await Task.WhenAny(
                        clients.Concat(new[] { acceptTask.AsTask() }));

                if (acceptTask.IsCompleted)
                {
                    var newClient = await acceptTask;

                    log.LogInformation(
                        "New Modbus client from {Remote}",
                        newClient.Client.RemoteEndPoint);

                    clients.Add(ClientTask(newClient, cancel));

                    acceptTask = listener.AcceptTcpClientAsync(cancel);
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
                        // Normal shutdown.
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

    private const byte ModbusFcReadHoldingRegisters = 0x03;

    private const byte ModbusExceptionIllegalFunction = 0x01;
    private const byte ModbusExceptionIllegalDataAddress = 0x02;
    private const byte ModbusExceptionIllegalDataValue = 0x03;

    private const int ModbusMaxHoldingRegisters = 65536;

    private readonly ConcurrentDictionary<ushort, ushort>
        holdingRegisterValues = new();

    public Task<ushort> SetHoldingRegister(ushort addr, ushort value)
    {
        holdingRegisterValues[addr] = value;

        return Task.FromResult((ushort)(addr + 1));
    }

    public Task<ushort> SetHoldingRegister(ushort addr, float value)
    {
        log.LogInformation(
            "SetHoldingRegister {Address} = {Value}",
            addr,
            value);

        var bits = BitConverter.SingleToUInt32Bits(value);

        // SunSpec FLOAT32:
        // high 16-bit word first, low 16-bit word second.
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

        var bytes = stringBytes
            .Concat(
                Enumerable.Range(
                        0,
                        maxBytes - stringBytes.Length)
                    .Select(_ => (byte)0))
            .ToArray();

        for (int i = 0; i < words; i++)
        {
            holdingRegisterValues[(ushort)(addr + i)] =
                BinaryPrimitives.ReadUInt16BigEndian(
                    bytes.AsSpan(i * 2));
        }

        return Task.FromResult((ushort)(addr + words));
    }

    private async Task ClientTask(
        TcpClient client,
        CancellationToken cancel)
    {
        var remote = client.Client.RemoteEndPoint?.ToString()
                     ?? "unknown";

        try
        {
            using (client)
            {
                var stream = client.GetStream();

                while (!cancel.IsCancellationRequested)
                {
                    /*
                     * Modbus TCP MBAP header:
                     *
                     * bytes 0-1 : Transaction ID
                     * bytes 2-3 : Protocol ID
                     * bytes 4-5 : Length
                     * byte  6   : Unit ID
                     *
                     * Length includes:
                     *   Unit ID + PDU
                     */

                    var mbap = new byte[7];

                    try
                    {
                        await stream.ReadExactlyAsync(
                            mbap,
                            cancel);
                    }
                    catch (EndOfStreamException)
                    {
                        log.LogInformation(
                            "Modbus client {Remote} disconnected",
                            remote);

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

                    var unitId = mbap[6];

                    if (protocolId != 0)
                    {
                        log.LogWarning(
                            "Invalid Modbus protocol ID from {Remote}: " +
                            "Transaction={Transaction}, " +
                            "Protocol={Protocol}, Unit={Unit}",
                            remote,
                            transactionId,
                            protocolId,
                            unitId);

                        break;
                    }

                    /*
                     * Length includes the Unit ID byte which
                     * we've already read as part of MBAP.
                     */
                    if (length < 2)
                    {
                        log.LogWarning(
                            "Invalid Modbus length from {Remote}: " +
                            "Transaction={Transaction}, " +
                            "Length={Length}, Unit={Unit}",
                            remote,
                            transactionId,
                            length,
                            unitId);

                        break;
                    }

                    var pduLength = length - 1;
                    var pdu = new byte[pduLength];

                    await stream.ReadExactlyAsync(
                        pdu,
                        cancel);

                    var function = pdu[0];

                    /*
                     * Log every request at Information level
                     * while troubleshooting the Fronius.
                     */
                    log.LogInformation(
                        "MODBUS RX {Remote}: " +
                        "TX={Transaction} Unit={Unit} " +
                        "FC=0x{Function:X2} Length={Length} " +
                        "PDU={Pdu}",
                        remote,
                        transactionId,
                        unitId,
                        function,
                        pduLength,
                        Convert.ToHexString(pdu));

                    switch (function)
                    {
                        case ModbusFcReadHoldingRegisters:
                            await HandleReadHoldingRegisters(
                                stream,
                                remote,
                                transactionId,
                                unitId,
                                pdu,
                                cancel);
                            break;

                        default:
                            log.LogWarning(
                                "Unsupported Modbus function from {Remote}: " +
                                "TX={Transaction} Unit={Unit} " +
                                "FC=0x{Function:X2}",
                                remote,
                                transactionId,
                                unitId,
                                function);

                            await SendExceptionResponse(
                                stream,
                                transactionId,
                                unitId,
                                function,
                                ModbusExceptionIllegalFunction,
                                cancel);

                            break;
                    }
                }
            }
        }
        catch (OperationCanceledException)
            when (cancel.IsCancellationRequested)
        {
            // Normal service shutdown.
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

    private async Task HandleReadHoldingRegisters(
        NetworkStream stream,
        string remote,
        ushort transactionId,
        byte unitId,
        byte[] pdu,
        CancellationToken cancel)
    {
        /*
         * FC03 request PDU:
         *
         * byte 0    : Function (03)
         * bytes 1-2 : Starting address
         * bytes 3-4 : Quantity
         */

        if (pdu.Length != 5)
        {
            log.LogWarning(
                "Invalid FC03 request length from {Remote}: " +
                "TX={Transaction} Unit={Unit} Length={Length}",
                remote,
                transactionId,
                unitId,
                pdu.Length);

            await SendExceptionResponse(
                stream,
                transactionId,
                unitId,
                ModbusFcReadHoldingRegisters,
                ModbusExceptionIllegalDataValue,
                cancel);

            return;
        }

        var addr =
            BinaryPrimitives.ReadUInt16BigEndian(
                pdu.AsSpan(1, 2));

        var num =
            BinaryPrimitives.ReadUInt16BigEndian(
                pdu.AsSpan(3, 2));

        log.LogInformation(
            "FC03 {Remote}: TX={Transaction} Unit={Unit} " +
            "Addr={Address} Count={Count} " +
            "(end={EndAddress})",
            remote,
            transactionId,
            unitId,
            addr,
            num,
            num > 0
                ? (int)addr + num - 1
                : addr);

        /*
         * Modbus specifies a maximum of 125 registers
         * in an FC03 request.
         */
        if (num == 0 || num > 125)
        {
            log.LogWarning(
                "Invalid FC03 register count from {Remote}: " +
                "Addr={Address} Count={Count}",
                remote,
                addr,
                num);

            await SendExceptionResponse(
                stream,
                transactionId,
                unitId,
                ModbusFcReadHoldingRegisters,
                ModbusExceptionIllegalDataValue,
                cancel);

            return;
        }

        if ((int)addr + num > ModbusMaxHoldingRegisters)
        {
            log.LogWarning(
                "FC03 address range outside Modbus address space " +
                "from {Remote}: Addr={Address} Count={Count}",
                remote,
                addr,
                num);

            await SendExceptionResponse(
                stream,
                transactionId,
                unitId,
                ModbusFcReadHoldingRegisters,
                ModbusExceptionIllegalDataAddress,
                cancel);

            return;
        }

        /*
         * For now, retain the emulator's previous behavior:
         *
         * registers that haven't explicitly been populated
         * are returned as zero.
         *
         * This is intentional while troubleshooting the
         * Fronius because changing this to Illegal Data
         * Address could alter meter-discovery behavior.
         */

        var responsePdu =
            new byte[2 + num * 2];

        responsePdu[0] =
            ModbusFcReadHoldingRegisters;

        responsePdu[1] =
            (byte)(num * 2);

        var missingRegisters =
            new List<ushort>();

        for (int i = 0; i < num; i++)
        {
            var regaddr =
                (ushort)(addr + i);

            ushort value;

            if (!holdingRegisterValues.TryGetValue(
                    regaddr,
                    out value))
            {
                value = 0;
                missingRegisters.Add(regaddr);
            }

            BinaryPrimitives.WriteUInt16BigEndian(
                responsePdu.AsSpan(2 + i * 2, 2),
                value);
        }

        if (missingRegisters.Count > 0)
        {
            log.LogInformation(
                "FC03 {Remote}: Unit={Unit} Addr={Address} " +
                "Count={Count}: {MissingCount} registers " +
                "were unset and returned as zero: {Missing}",
                remote,
                unitId,
                addr,
                num,
                missingRegisters.Count,
                FormatRegisterRanges(missingRegisters));
        }

        await SendResponse(
            stream,
            transactionId,
            unitId,
            responsePdu,
            cancel);

        log.LogInformation(
            "MODBUS TX {Remote}: TX={Transaction} Unit={Unit} " +
            "FC=0x03 Addr={Address} Count={Count} OK",
            remote,
            transactionId,
            unitId,
            addr,
            num);
    }

    private async Task SendExceptionResponse(
        NetworkStream stream,
        ushort transactionId,
        byte unitId,
        byte function,
        byte exceptionCode,
        CancellationToken cancel)
    {
        var responsePdu = new byte[]
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
            "MODBUS TX exception: TX={Transaction} Unit={Unit} " +
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
        /*
         * MBAP length =
         *   Unit ID (1 byte) + PDU length
         */
        var response =
            new byte[7 + pdu.Length];

        BinaryPrimitives.WriteUInt16BigEndian(
            response.AsSpan(0, 2),
            transactionId);

        // Modbus TCP protocol ID = 0
        BinaryPrimitives.WriteUInt16BigEndian(
            response.AsSpan(2, 2),
            0);

        BinaryPrimitives.WriteUInt16BigEndian(
            response.AsSpan(4, 2),
            (ushort)(1 + pdu.Length));

        response[6] = unitId;

        pdu.CopyTo(
            response.AsSpan(7));

        await stream.WriteAsync(
            response,
            cancel);
    }

    private static string FormatRegisterRanges(
        List<ushort> registers)
    {
        if (registers.Count == 0)
        {
            return "";
        }

        var ranges = new List<string>();

        ushort start = registers[0];
        ushort previous = registers[0];

        for (int i = 1; i < registers.Count; i++)
        {
            var current = registers[i];

            if (current == previous + 1)
            {
                previous = current;
                continue;
            }

            ranges.Add(
                start == previous
                    ? start.ToString()
                    : $"{start}-{previous}");

            start = current;
            previous = current;
        }

        ranges.Add(
            start == previous
                ? start.ToString()
                : $"{start}-{previous}");

        return string.Join(", ", ranges);
    }
}