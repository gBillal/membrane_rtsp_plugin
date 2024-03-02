defmodule Membrane.RTSP.Source do
  @moduledoc """
  Source bin responsible for connecting to an RTSP server.

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
                default: [:video],
                description: """
                The type of streams to read from the RTSP server.
                By default only video are streamed
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
    opts = Map.take(state, [:stream_uri, :allowed_media_types])
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
      {[notify_parent: {:new_track, ssrc, track}], %{state | ssrc_to_track: ssrc_to_track}}
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

    spec = [
      get_child(:rtp_session)
      |> via_out(Pad.ref(:output, ssrc), options: [depayloader: get_rtp_depayloader(track)])
      |> child({:rtp_parser, ssrc}, get_parser(track))
      |> bin_output(pad)
    ]

    {[spec: spec], state}
  end

  @impl true
  def handle_info({:tracks, tracks, transport}, _ctx, state) do
    Membrane.Logger.info("Received tracks: #{inspect(tracks)}")

    fmt_mapping =
      Enum.map(tracks, fn %{rtpmap: rtpmap} ->
        {rtpmap.payload_type, {String.to_atom(rtpmap.encoding), rtpmap.clock_rate}}
      end)
      |> Enum.into(%{})

    spec = [
      child(:source, %Membrane.TCP.Source{connection_side: :client, local_socket: transport})
      |> child(:tcp_depayloader, Decapsulator)
      |> via_in(Pad.ref(:rtp_input, make_ref()))
      |> child(:rtp_session, %Membrane.RTP.SessionBin{fmt_mapping: fmt_mapping})
    ]

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

  defp get_parser(%{rtpmap: %{encoding: "H264"}} = track) do
    sps = track.fmtp.sprop_parameter_sets && track.fmtp.sprop_parameter_sets.sps
    pps = track.fmtp.sprop_parameter_sets && track.fmtp.sprop_parameter_sets.pps

    %Membrane.H264.Parser{spss: List.wrap(sps), ppss: List.wrap(pps), repeat_parameter_sets: true}
  end

  defp get_parser(%{rtpmap: %{encoding: "H265"}} = track) do
    %Membrane.H265.Parser{
      vpss: List.wrap(track.fmtp.sprop_vps) |> Enum.map(&clean_parameter_set/1),
      spss: List.wrap(track.fmtp.sprop_sps) |> Enum.map(&clean_parameter_set/1),
      ppss: List.wrap(track.fmtp.sprop_pps) |> Enum.map(&clean_parameter_set/1),
      repeat_parameter_sets: true
    }
  end

  defp get_parser(_track), do: nil

  # a strange issue with one of Milesight camera where the parameter sets has
  # <<0, 0, 0, 1>> at the end
  defp clean_parameter_set(ps) do
    case :binary.part(ps, byte_size(ps), -4) do
      <<0, 0, 0, 1>> -> :binary.part(ps, 0, byte_size(ps) - 4)
      _other -> ps
    end
  end
end
