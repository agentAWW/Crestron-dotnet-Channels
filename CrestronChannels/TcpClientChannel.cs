using Crestron.SimplSharp;

using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;

using IPAddress = System.Net.IPAddress;

namespace CrestronChannels;

/// <summary>Creates a TcpClient with .NET channels for easy management.</summary>
public class TcpClientChannel
{
	readonly Channel<byte[]> _txChannel = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
	{
		SingleReader = true,
		SingleWriter = false,
		AllowSynchronousContinuations = false
	});
	readonly Channel<byte[]> _rxChannel = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
	{
		SingleReader = false,
		SingleWriter = true,
		AllowSynchronousContinuations = false
	});

	/// <summary>Gets the writer to send data out of the ComPort.</summary>
	public ChannelWriter<byte[]> Tx { get => _txChannel.Writer; }
	/// <summary>Gets the reader to receive data from the ComPort.</summary>
	public ChannelReader<byte[]> Rx { get => _rxChannel.Reader; }

	/// <summary>Starts listening for data from the specified TCP server from the specified port. Will return once connection is closed.</summary>
	/// <param name="ipAddress"></param>
	/// <param name="portNumber"></param>
	/// <param name="ct"></param>
	/// <returns></returns>
	public async Task ConnectAsync(IPAddress ipAddress, int portNumber, CancellationToken ct)
	{
		while (!ct.IsCancellationRequested)
		{
			await Task.Delay(5000, ct); // Wait before retrying
			using TcpClient tcpClient = new();
			using CancellationTokenSource ioCancel = new();
			try
			{
				await tcpClient.ConnectAsync(ipAddress, portNumber, ct);
				ErrorLog.Notice("Connected to server ({0}:{1})", ipAddress, portNumber);
				//tcpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
				//tcpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.TcpKeepAliveTime, 30);
				//tcpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.TcpKeepAliveRetryCount, 3);
				//tcpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.TcpKeepAliveInterval, 3);
				//ErrorLog.Notice("Keep-alive options set for server ({0}:{1})", ipAddress, portNumber);
				await using NetworkStream stream = tcpClient.GetStream();
				Task txOps = WriteToSocketAsync(stream, ioCancel.Token);
				Task rxOps = ReadFromSocketAsync(stream, ioCancel.Token);
				ErrorLog.Notice("Started read/write tasks for server ({0}:{1})", ipAddress, portNumber);
				await Task.WhenAny([txOps, rxOps]);
				ErrorLog.Notice("Wrapping up...");
			}
			catch (OperationCanceledException) { }
			catch (Exception ex)
			{
				ErrorLog.Error($"Error connecting to server ({ipAddress}:{portNumber}): {ex.Message}");
			}
			finally
			{
				//CrestronConsole.PrintLine("Leaving the connection...");
				ioCancel.Cancel();
			}
		}
		_rxChannel.Writer.TryComplete();
	}

	private static string HexFrom8Bit(string str)
	{
		StringBuilder sb = new();
		foreach (char c in str)
		{ sb.AppendFormat("{0:X2}", (int)c); }
		return sb.ToString();
	}

	private async Task WriteToSocketAsync(NetworkStream stream, CancellationToken ct)
	{
		try
		{
			await foreach (byte[] bytes in _txChannel.Reader.ReadAllAsync(ct))
			{
				//CrestronConsole.PrintLine($"Received data to send: {HexFrom8Bit(Encoding.Latin1.GetString(bytes))}");
				await stream.WriteAsync(bytes, ct);
				//CrestronConsole.PrintLine($"Sent data to socket.");
			}
		}
		catch (OperationCanceledException) { }
		catch (Exception ex)
		{
			//CrestronConsole.PrintLine($"Error writing to socket: {ex.Message}");
		}
		finally
		{
			//CrestronConsole.PrintLine("Leaving the writer...");
		}
	}

	private async Task ReadFromSocketAsync(NetworkStream stream, CancellationToken ct)
	{
		byte[] tcpBuffer = new byte[1024];

		try
		{
			int bytesRead = await stream.ReadAsync(tcpBuffer, ct);
			//CrestronConsole.PrintLine($"Read {bytesRead} bytes from socket.");
			while (bytesRead > 0)
			{
				_rxChannel.Writer.TryWrite(tcpBuffer[..bytesRead]);
				bytesRead = await stream.ReadAsync(tcpBuffer, ct);
				//CrestronConsole.PrintLine($"Read {bytesRead} bytes from socket.");
			}
		}
		catch (OperationCanceledException) { }
		catch (Exception ex) { ErrorLog.Error($"Error reading from the socket: {ex.Message}"); }
		finally
		{
			//CrestronConsole.PrintLine("Leaving the reader...");
		}
	}
}
