defmodule Membrane.RTSP.Source.Decapsulator do
  @moduledoc false

  use Membrane.Filter

  alias Membrane.{Buffer, RemoteStream, RTP, RTSP}

  def_options rtsp_session: [
                spec: pid() | nil,
                default: nil,
                description: """
                PID of a RTSP Session (returned from Membrane.RTSP.start or Membrane.RTSP.start_link)
                that received RTSP responses will be forwarded to. If nil the responses will be
                discarded.
                """
              ]

  def_input_pad :input, accepted_format: %RemoteStream{type: :bytestream}

  def_output_pad :output, accepted_format: %RemoteStream{type: :packetized, content_format: RTP}

  @impl true
  def handle_init(_ctx, opts) do
    state =
      Map.from_struct(opts)
      |> Map.merge(%{
        unprocessed_data: <<>>
      })

    {[], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    stream_format = %RemoteStream{type: :packetized, content_format: RTP}
    {[stream_format: {:output, stream_format}], state}
  end

  @impl true
  def handle_stream_format(:input, _stream_format, _ctx, state) do
    {[], state}
  end

  @impl true
  def handle_buffer(:input, %Buffer{payload: payload, metadata: metadata}, _ctx, state) do
    unprocessed_data =
      if rtsp_response?(state.unprocessed_data, payload) do
        if state.rtsp_session != nil do
          {:ok, %RTSP.Response{status: 200}} =
            RTSP.handle_response(state.rtsp_session, state.unprocessed_data)
        end

        <<>>
      else
        state.unprocessed_data
      end

    packets_binary = unprocessed_data <> payload

    {unprocessed_data, complete_packets_binaries} = get_complete_packets(packets_binary)

    packets_buffers =
      Enum.map(complete_packets_binaries, &%Buffer{payload: &1, metadata: metadata})

    {[buffer: {:output, packets_buffers}], %{state | unprocessed_data: unprocessed_data}}
  end

  @spec rtsp_response?(binary(), binary()) :: boolean()
  defp rtsp_response?(maybe_rtsp_response, new_payload) do
    String.starts_with?(new_payload, "$") and String.starts_with?(maybe_rtsp_response, "RTSP")
  end

  @spec get_complete_packets(binary()) ::
          {unprocessed_data :: binary(), complete_packets :: [binary()]}
  defp get_complete_packets(packets_binary, complete_packets \\ [])

  defp get_complete_packets(packets_binary, complete_packets)
       when byte_size(packets_binary) <= 4 do
    {packets_binary, Enum.reverse(complete_packets)}
  end

  defp get_complete_packets(
         <<"$", _received_channel_id, payload_length::size(16), rest::binary>> = packets_binary,
         complete_packets
       ) do
    case rest do
      <<complete_packet_binary::binary-size(payload_length)-unit(8), rest::binary>> ->
        complete_packets = [complete_packet_binary | complete_packets]
        get_complete_packets(rest, complete_packets)

      _incomplete_packet_binary ->
        {packets_binary, Enum.reverse(complete_packets)}
    end
  end

  defp get_complete_packets(rtsp_message, _complete_packets_binaries) do
    # If the payload doesn't start with a "$" then it must be a RTSP message (or a part of it)
    {rtsp_message, []}
  end
end
