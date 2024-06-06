defmodule Membrane.RTSP.Source do
  @moduledoc """
  Source bin responsible for connecting to an RTSP server.

  This element connects to an RTSP server, depayload and parses the received media if possible.
  If there's no suitable depayloader and parser, the raw payload is sent to the subsequent elements in
  the pipeline.

  In case connection can't be established or is severed during streaming this bin will crash.

  The following codecs are depayloaded and parsed:
    * `H264`
    * `H265`

  ### Notifications
    * `{:new_track, ssrc, track}` - sent when the track is parsed and available for consumption by the next
    elements. An output pad `Pad.ref(:output, ssrc)` should be linked to receive the data.
  """

  use Membrane.Bin

  require Membrane.Logger

  alias __MODULE__
  alias __MODULE__.{ConnectionManager, ReadyNotifier}
  alias Membrane.RTSP.TCP.Decapsulator
  alias Membrane.Time

  @type transport ::
          {:udp, port_range_start :: non_neg_integer(), port_range_end :: non_neg_integer()}
          | :tcp

  def_options stream_uri: [
                spec: binary(),
                description: "The RTSP URI of the resource to stream."
              ],
              allowed_media_types: [
                spec: [:video | :audio | :application],
                default: [:video, :audio, :application],
                description: """
                The media type to accept from the RTSP server.
                """
              ],
              transport: [
                spec: transport(),
                default: :tcp,
                description: """
                Transport protocol that will be used in the established RTSP stream. In case of
                UDP a range needs to provided from which receiving ports will be chosen.
                """
              ],
              timeout: [
                spec: Time.t(),
                default: Time.seconds(15),
                default_inspector: &Time.pretty_duration/1,
                description: "RTSP response timeout"
              ],
              keep_alive_interval: [
                spec: Time.t(),
                default: Time.seconds(15),
                default_inspector: &Time.pretty_duration/1,
                description: """
                Interval of a heartbeat sent to the RTSP server at a regular interval to
                keep the session alive.
                """
              ]

  def_output_pad :output,
    accepted_format: _any,
    availability: :on_request

  defmodule State do
    @moduledoc false
    @type t :: %__MODULE__{
            stream_uri: binary(),
            allowed_media_types: ConnectionManager.media_types(),
            transport: Source.transport(),
            timeout: Time.t(),
            keep_alive_interval: Time.t(),
            connection_manager: ConnectionManager.t(),
            tracks: [ConnectionManager.track()],
            ssrc_to_track: %{non_neg_integer() => ConnectionManager.track()}
          }

    @enforce_keys [:stream_uri, :allowed_media_types, :transport, :timeout, :keep_alive_interval]
    defstruct @enforce_keys ++
                [
                  connection_manager: nil,
                  tracks: [],
                  ssrc_to_track: %{}
                ]
  end

  defmodule ReadyNotifier do
    @moduledoc false
    # This element's only purpose is to send a notification to it's parent when it's entered playing
    # state, meaning all it's siblings also did.
    use Membrane.Source

    @impl true
    def handle_playing(_ctx, state) do
      {[notify_parent: :ready], state}
    end
  end

  @impl true
  def handle_init(_ctx, options) do
    state = struct(State, Map.from_struct(options))

    {[], state}
  end

  @impl true
  def handle_setup(_ctx, state) do
    opts =
      Map.take(state, [
        :stream_uri,
        :allowed_media_types,
        :transport,
        :timeout,
        :keep_alive_interval
      ])

    {:ok, connection_manager} = ConnectionManager.start_link(opts)
    {[], %{state | connection_manager: connection_manager}}
  end

  @impl true
  def handle_child_notification(
        {:new_rtp_stream, ssrc, pt, _extensions},
        :rtp_session,
        _ctx,
        state
      ) do
    case Enum.find(state.tracks, fn track -> track.rtpmap.payload_type == pt end) do
      nil ->
        {[], state}

      track ->
        ssrc_to_track = Map.put(state.ssrc_to_track, ssrc, track)

        {[notify_parent: {:new_track, ssrc, Map.delete(track, :transport)}],
         %{state | ssrc_to_track: ssrc_to_track}}
    end
  end

  @impl true
  def handle_child_notification({:request_socket_control, _socket, pid}, :tcp_source, _ctx, state) do
    ConnectionManager.transfer_rtsp_socket_control(state.connection_manager, pid)
    {[], state}
  end

  @impl true
  def handle_child_notification(:ready, :ready_notifier, _ctx, state) do
    send(state.connection_manager, :source_ready)
    {[], state}
  end

  @impl true
  def handle_child_notification(_notification, _element, _ctx, state) do
    {[], state}
  end

  @impl true
  def handle_info(%{tracks: tracks}, _ctx, state) do
    fmt_mapping =
      Enum.map(tracks, fn %{rtpmap: rtpmap} ->
        {rtpmap.payload_type, {String.to_atom(rtpmap.encoding), rtpmap.clock_rate}}
      end)
      |> Enum.into(%{})

    common_spec = [
      child(:rtp_session, %Membrane.RTP.SessionBin{fmt_mapping: fmt_mapping}),
      child(:ready_notifier, ReadyNotifier)
    ]

    case state.transport do
      :tcp ->
        {:tcp, socket} = List.first(tracks).transport

        spec =
          common_spec ++
            [
              child(:tcp_source, %Membrane.TCP.Source{
                connection_side: :client,
                local_socket: socket
              })
              |> child(:tcp_depayloader, Decapsulator)
              |> via_in(Pad.ref(:rtp_input, make_ref()))
              |> get_child(:rtp_session)
            ]

        {[spec: spec], %{state | tracks: tracks}}

      {:udp, _port_range_start, _port_range_end} ->
        spec =
          common_spec ++
            Enum.flat_map(tracks, fn track ->
              {:udp, rtp_port, rtcp_port} = track.transport

              [
                child({:udp, make_ref()}, %Membrane.UDP.Source{local_port_no: rtp_port})
                |> via_in(Pad.ref(:rtp_input, make_ref()))
                |> get_child(:rtp_session),
                child({:udp, make_ref()}, %Membrane.UDP.Source{local_port_no: rtcp_port})
                |> via_in(Pad.ref(:rtp_input, make_ref()))
                |> get_child(:rtp_session)
              ]
            end)

        {[spec: spec], %{state | tracks: tracks}}
    end
  end

  @impl true
  def handle_info(_message, _ctx, state) do
    {[], state}
  end

  @impl true
  def handle_pad_added(Pad.ref(:output, ssrc) = pad, _ctx, state) do
    track = Map.fetch!(state.ssrc_to_track, ssrc)

    spec =
      get_child(:rtp_session)
      |> via_out(Pad.ref(:output, ssrc), options: [depayloader: get_rtp_depayloader(track)])
      |> parser(track)
      |> bin_output(pad)

    {[spec: spec], state}
  end

  @impl true
  def handle_terminate_request(_ctx, state) do
    ConnectionManager.stop(state.connection_manager)
    {[terminate: :normal], state}
  end

  @spec get_rtp_depayloader(ConnectionManager.track()) :: module() | nil
  defp get_rtp_depayloader(%{rtpmap: %{encoding: "H264"}}), do: Membrane.RTP.H264.Depayloader
  defp get_rtp_depayloader(%{rtpmap: %{encoding: "H265"}}), do: Membrane.RTP.H265.Depayloader
  defp get_rtp_depayloader(_track), do: nil

  @spec parser(ChildrenSpec.builder(), ConnectionManager.track()) :: ChildrenSpec.builder()
  defp parser(link_builder, %{rtpmap: %{encoding: "H264"}} = track) do
    sps = track.fmtp.sprop_parameter_sets && track.fmtp.sprop_parameter_sets.sps
    pps = track.fmtp.sprop_parameter_sets && track.fmtp.sprop_parameter_sets.pps

    child(link_builder, {:parser, make_ref()}, %Membrane.H264.Parser{
      spss: List.wrap(sps),
      ppss: List.wrap(pps),
      repeat_parameter_sets: true
    })
  end

  defp parser(link_builder, %{rtpmap: %{encoding: "H265"}} = track) do
    child(link_builder, {:parser, make_ref()}, %Membrane.H265.Parser{
      vpss: List.wrap(track.fmtp && track.fmtp.sprop_vps) |> Enum.map(&clean_parameter_set/1),
      spss: List.wrap(track.fmtp && track.fmtp.sprop_sps) |> Enum.map(&clean_parameter_set/1),
      ppss: List.wrap(track.fmtp && track.fmtp.sprop_pps) |> Enum.map(&clean_parameter_set/1),
      repeat_parameter_sets: true
    })
  end

  defp parser(link_builder, _track), do: link_builder

  # a strange issue with one of Milesight camera where the parameter sets has
  # <<0, 0, 0, 1>> at the end
  @spec clean_parameter_set(binary()) :: binary()
  defp clean_parameter_set(ps) do
    case :binary.part(ps, byte_size(ps), -4) do
      <<0, 0, 0, 1>> -> :binary.part(ps, 0, byte_size(ps) - 4)
      _other -> ps
    end
  end
end
