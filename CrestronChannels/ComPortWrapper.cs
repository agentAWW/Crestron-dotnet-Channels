using Crestron.SimplSharpPro;

using System.Threading.Channels;

namespace CrestronChannels;

/// <summary>A wrapper for Crestron ComPorts to be used in an asynchronous manner using .NET Channels.</summary>
public class ComPortWrapper
{
	readonly ComPort _port;
	readonly Channel<string> _txChannel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
	{
		SingleReader = true,
		SingleWriter = false,
		AllowSynchronousContinuations = false
	});
	readonly Channel<string> _rxChannel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
	{
		SingleReader = false,
		SingleWriter = true,
		AllowSynchronousContinuations = false
	});

	/// <summary>Gets the writer to send data out of the ComPort.</summary>
	public ChannelWriter<string> Tx { get => _txChannel.Writer; }
	/// <summary>Gets the reader to receive data from the ComPort.</summary>
	public ChannelReader<string> Rx { get => _rxChannel.Reader; }

	/// <summary>Initializes a new wrapper for a Crestron ComPort.</summary>
	/// <param name="port">The Crestron ComPort to connect with channels.</param>
	public ComPortWrapper(ComPort port)
	{
		_port = port;
		_port.SerialDataReceived += OnComPortReceive;
	}

	/// <summary>Starts listening for data to send out the com port. When cancelled, it will stop listening and send a completion signal on the receive channel.</summary>
	/// <param name="ct">A cancellation token to stop listening for outgoing data.</param>
	/// <returns></returns>
	public async Task ConnectAsync(CancellationToken ct)
	{
		try
		{
			await foreach (string txData in _txChannel.Reader.ReadAllAsync(ct))
			{ _port.Send(txData); }
		}
		catch (OperationCanceledException) { }
		finally { _rxChannel.Writer.TryComplete(); }
	}

	private void OnComPortReceive(ComPort rxComPort, ComPortSerialDataEventArgs args) => _ = _rxChannel.Writer.TryWrite(args.SerialData);
}
