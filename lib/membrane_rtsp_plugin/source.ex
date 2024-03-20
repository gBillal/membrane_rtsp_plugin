defmodule Membrane.RTSP.Source do
  @moduledoc """
  Source bin responsible for connecting to an RTSP server.

  This element connects to an RTSP server, depayload and parse the received media if possible.
  If there's no suitable depayloader and parser, the raw payload is sent to the subsequent elements in
  the pipeline.

  The following codecs are depayloaded and parsed:
    * `H264`
    * `H265`

  ### Notifications
    * `{:new_track, ssrc, track}` - sent when the track is parsed and available for consumption by the upper
    elements. The output pad should be linked to receive the data.
    * `{:connection_failed, reason}` - sent when the element cannot establish connection or a connection is lost
    during streaming. This element will try to reconnect to the server, this event is sent only once even if the error
    persist.
  """

  use Membrane.Bin

  require Membrane.Logger

  alias __MODULE__.{ConnectionManager, Decapsulator}

  def_options stream_uri: [
                spec: binary(),
                description: "The RTSP uri of the resource to stream."
              ],
              allowed_media_types: [
                spec: [:video | :audio | :application],
                default: [:video, :audio, :application],
                description: """
                The media type to accept from the RTSP server.
                """
              ],
              transport: [
                spec: [:udp | :tcp],
                default: :tcp,
                description: "Set the rtsp transport protocol."
              ],
              timeout: [
                spec: non_neg_integer(),
                default: :timer.seconds(15),
                description: "Set RTSP response timeout"
              ],
              keep_alive_interval: [
                spec: non_neg_integer(),
                default: :timer.seconds(15),
                description: """
                Send a heartbeat to the RTSP server at a regular interval to
                keep the session alive.
                """
              ]

  def_output_pad :output,
    accepted_format: _any,
    availability: :on_request

  @impl true
  def handle_init(_ctx, options) do
    state =
      options
      |> Map.from_struct()
      |> Map.merge(%{connection_manager: nil, tracks: [], ssrc_to_track: %{}})

    {[], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
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
    if track = Enum.find(state.tracks, fn track -> track.rtpmap.payload_type == pt end) do
      ssrc_to_track = Map.put(state.ssrc_to_track, ssrc, track)

      {[notify_parent: {:new_track, ssrc, Map.delete(track, :transport)}],
       %{state | ssrc_to_track: ssrc_to_track}}
    else
      {[], state}
    end
  end

  @impl true
  def handle_child_notification(_notification, _element, _ctx, state) do
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
  def handle_info({:tracks, tracks}, _ctx, state) do
    Membrane.Logger.info("Received tracks: #{inspect(tracks)}")

    fmt_mapping =
      Enum.map(tracks, fn %{rtpmap: rtpmap} ->
        {rtpmap.payload_type, {String.to_atom(rtpmap.encoding), rtpmap.clock_rate}}
      end)
      |> Enum.into(%{})

    spec =
      case state.transport do
        :tcp ->
          local_socket = List.first(tracks).transport

          child(:source, %Membrane.TCP.Source{
            connection_side: :client,
            local_socket: local_socket
          })
          |> child(:tcp_depayloader, Decapsulator)
          |> via_in(Pad.ref(:rtp_input, make_ref()))
          |> child(:rtp_session, %Membrane.RTP.SessionBin{fmt_mapping: fmt_mapping})

        :udp ->
          [child(:rtp_session, %Membrane.RTP.SessionBin{fmt_mapping: fmt_mapping})] ++
            Enum.flat_map(tracks, fn track ->
              {rtp_port, rtcp_port} = track.transport

              [
                child({:udp, make_ref()}, %Membrane.UDP.Source{local_port_no: rtp_port})
                |> via_in(Pad.ref(:rtp_input, make_ref()))
                |> get_child(:rtp_session),
                child({:udp, make_ref()}, %Membrane.UDP.Source{local_port_no: rtcp_port})
                |> via_in(Pad.ref(:rtp_input, make_ref()))
                |> get_child(:rtp_session)
              ]
            end)
      end

    {[spec: spec], %{state | tracks: tracks}}
  end

  @impl true
  def handle_info({:connection_failed, reason}, ctx, state) do
    {[remove_children: Map.keys(ctx.children), notify_parent: {:connection_failed, reason}],
     state}
  end

  @impl true
  def handle_info(_message, _ctx, state) do
    {[], state}
  end

  defp get_rtp_depayloader(%{rtpmap: %{encoding: "H264"}}), do: Membrane.RTP.H264.Depayloader
  defp get_rtp_depayloader(%{rtpmap: %{encoding: "H265"}}), do: Membrane.RTP.H265.Depayloader
  defp get_rtp_depayloader(_track), do: nil

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
  defp clean_parameter_set(ps) do
    case :binary.part(ps, byte_size(ps), -4) do
      <<0, 0, 0, 1>> -> :binary.part(ps, 0, byte_size(ps) - 4)
      _other -> ps
    end
  end
end
