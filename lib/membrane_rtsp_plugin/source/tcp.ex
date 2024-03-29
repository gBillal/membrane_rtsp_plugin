defmodule Membrane.RTSP.Source.TCP do
  @moduledoc """
  Element that reads packets from a TCP socket and sends their payloads through the output pad.
  """
  use Membrane.Source

  require Membrane.Logger
  alias Membrane.RemoteStream

  def_options local_socket: [
                spec: :gen_tcp.socket(),
                description: "Already connected TCP socket."
              ]

  def_output_pad :output, accepted_format: %RemoteStream{type: :bytestream}, flow_control: :push

  @impl true
  def handle_init(_context, opts) do
    {:ok, {peer_address, peer_port_no}} = :inet.peername(opts.local_socket)

    state = %{
      local_socket: opts.local_socket,
      peer_address: peer_address,
      peer_port_no: peer_port_no
    }

    {[], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    {[stream_format: {:output, %RemoteStream{type: :bytestream}}, notify_parent: {:pid, self()}],
     state}
  end

  @impl true
  def handle_info({:tcp, _socket, msg}, _ctx, state) do
    metadata =
      Map.new()
      |> Map.put(:tcp_source_address, state.peer_address)
      |> Map.put(:tcp_source_port, state.peer_port_no)
      |> Map.put(:arrival_ts, Membrane.Time.vm_time())

    {[buffer: {:output, %Membrane.Buffer{payload: msg, metadata: metadata}}], state}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, _ctx, state) do
    {[end_of_stream: :output], state}
  end

  @impl true
  def handle_info({:tcp_error, _socket, reason}, _ctx, _state) do
    raise "TCP Socket receiving error, reason: #{inspect(reason)}"
  end

  @impl true
  def handle_info(msg, _ctx, state) do
    Membrane.Logger.warning("Received unexpected message: #{inspect(msg)}")
    {[], state}
  end

  @impl true
  def handle_terminate_request(_ctx, state) do
    :gen_tcp.close(state.local_socket)
    {[terminate: :normal], state}
  end
end
