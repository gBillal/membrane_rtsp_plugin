defmodule Membrane.RTSP.TCP.Sink do
  @moduledoc false

  use Membrane.Sink

  def_input_pad(:input, accepted_format: _any, availability: :on_request)

  def_options(socket: [spec: :inet.socket()])

  @impl true
  def handle_init(_ctx, opts) do
    {[], %{socket: opts.socket}}
  end

  @impl true
  def handle_buffer(Pad.ref(:input, channel), buffer, _ctx, state) do
    :gen_tcp.send(
      state.socket,
      <<0x24::8, channel::8, byte_size(buffer.payload)::16, buffer.payload::binary>>
    )

    {[], state}
  end
end

defmodule Membrane.RTSP.UDP.Sink do
  @moduledoc false

  use Membrane.Sink

  def_input_pad(:input, accepted_format: _any)

  def_options(
    socket: [spec: :inet.socket()],
    address: [spec: :inet.ipaddress()],
    port: [spec: :inet.port_number()]
  )

  @impl true
  def handle_init(_ctx, opts) do
    {[], %{socket: opts.socket, address: opts.address, port: opts.port}}
  end

  @impl true
  def handle_buffer(:input, buffer, _ctx, state) do
    :gen_udp.send(state.socket, state.address, state.port, buffer.payload)
    {[], state}
  end
end

defmodule Membrane.RTP.Plain.Payloader do
  @moduledoc false
  use Membrane.Filter

  alias Membrane.RTP

  def_input_pad :input, accepted_format: _any
  def_output_pad :output, accepted_format: RTP

  @impl true
  def handle_init(_ctx, _opts), do: {[], nil}

  @impl true
  def handle_playing(_ctx, state) do
    {[stream_format: {:output, %RTP{}}], state}
  end

  @impl true
  def handle_stream_format(:input, _stream_format, _context, state) do
    {[], state}
  end

  @impl true
  def handle_buffer(:input, buffer, _ctx, state) do
    {[buffer: {:output, %{buffer | dts: 0, pts: 0}}], state}
  end
end

defmodule Membrane.RTSP.ServerPipeline do
  @moduledoc false

  use Membrane.Pipeline

  @rtp_payloader %{
    H264: Membrane.RTP.H264.Payloader,
    H265: Membrane.RTP.H265.Payloader,
    plain: Membrane.RTP.Plain.Payloader
  }

  @spec start_link(list()) :: Membrane.Pipeline.on_start()
  def start_link(sources) do
    Membrane.Pipeline.start_link(__MODULE__, sources)
  end

  @impl true
  def handle_setup(_ctx, state) do
    Process.sleep(100)
    {[], state}
  end

  @impl true
  def handle_init(_ctx, sources) do
    spec =
      [child(:session_bin, Membrane.RTP.SessionBin)] ++
        Enum.flat_map(sources, fn source ->
          [
            child({:source, make_ref()}, %Membrane.File.Source{location: source[:location]})
            |> parser(source[:encoding])
            |> via_in(Pad.ref(:input, source[:ssrc]),
              options: [payloader: Map.get(@rtp_payloader, source[:encoding])]
            )
            |> get_child(:session_bin)
            |> via_out(Pad.ref(:rtp_output, source[:ssrc]),
              options: [payload_type: source[:payload_type], clock_rate: source[:clock_rate]]
            )
            |> build_sink(source)
          ]
        end)

    {[spec: spec], %{total_sources: length(sources), ended_stream: 0}}
  end

  @impl true
  def handle_element_end_of_stream({:sink, _ref}, _pad, _ctx, state) do
    if state.ended_stream + 1 == state.total_sources do
      {[terminate: :shutdown], state}
    else
      {[], %{state | ended_stream: state.ended_stream + 1}}
    end
  end

  @impl true
  def handle_element_end_of_stream(_element, _pad, _ctx, state) do
    {[], state}
  end

  defp parser(link_builder, :H264) do
    child(link_builder, {:parser, make_ref()}, %Membrane.H264.Parser{
      generate_best_effort_timestamps: %{framerate: {60, 1}},
      output_stream_structure: :annexb,
      output_alignment: :nalu
    })
  end

  defp parser(link_builder, :H265) do
    child(link_builder, {:parser, make_ref()}, %Membrane.H265.Parser{
      generate_best_effort_timestamps: %{framerate: {60, 1}},
      output_stream_structure: :annexb,
      output_alignment: :nalu
    })
  end

  defp parser(link_builder, _encoding), do: link_builder

  defp build_sink(link_builder, options) do
    if options[:transport] == :TCP do
      link_builder
      |> via_in(Pad.ref(:input, elem(options.channels, 0)))
      |> child({:sink, make_ref()}, %Membrane.RTSP.TCP.Sink{socket: options[:tcp_socket]})
    else
      link_builder
      |> child({:sink, make_ref()}, %Membrane.RTSP.UDP.Sink{
        socket: options[:rtp_socket],
        address: options[:address],
        port: elem(options[:client_port], 0)
      })
    end
  end
end
