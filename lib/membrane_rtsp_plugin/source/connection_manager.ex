defmodule Membrane.RTSP.Source.ConnectionManager do
  @moduledoc false

  use GenServer

  require Membrane.Logger

  alias __MODULE__
  alias Membrane.RTSP

  @content_type_header [{"accept", "application/sdp"}]

  @type t() :: pid()
  @type media_types :: [:video | :audio | :application]
  @type connection_opts :: %{stream_uri: binary(), allowed_media_types: media_types()}
  @type track_transport ::
          {:tcp, :gen_tcp.socket()}
          | {:udp, rtp_port :: :inet.port_number(), rtcp_port :: :inet.port_number()}
  @type track :: %{
          control_path: String.t(),
          type: :video | :audio | :application,
          fmtp: ExSDP.Attribute.FMTP.t() | nil,
          rtpmap: ExSDP.Attribute.RTPMapping.t() | nil,
          transport: track_transport()
        }

  defmodule State do
    @moduledoc false
    @type t :: %__MODULE__{
            stream_uri: binary(),
            allowed_media_types: ConnectionManager.media_types(),
            transport: RTSP.Source.transport(),
            timeout: Membrane.Time.t(),
            keep_alive_interval: Membrane.Time.t(),
            parent_pid: pid(),
            rtsp_session: RTSP.t() | nil,
            tracks: [ConnectionManager.track()],
            keep_alive_timer: reference()
          }

    @enforce_keys [
      :stream_uri,
      :allowed_media_types,
      :transport,
      :timeout,
      :keep_alive_interval,
      :parent_pid
    ]
    defstruct @enforce_keys ++
                [
                  rtsp_session: nil,
                  tracks: [],
                  keep_alive_timer: nil
                ]
  end

  @typep connection_establishment_phase_return() ::
           {:ok, State.t()} | {:error, reason :: term(), State.t()}

  @spec start_link(connection_opts()) :: GenServer.on_start()
  def start_link(options) do
    GenServer.start_link(__MODULE__, Map.put(options, :parent_pid, self()))
  end

  @spec stop(pid()) :: :ok
  def stop(server) do
    GenServer.call(server, :stop)
  end

  @spec transfer_rtsp_socket_control(connection_manager :: pid(), new_controller :: pid()) :: :ok
  def transfer_rtsp_socket_control(connection_manager, new_controller) do
    GenServer.call(connection_manager, {:transfer_rtsp_socket_control, new_controller})
  end

  @impl true
  def init(options) do
    state = struct(State, options)
    send(self(), :connect)
    {:ok, state}
  end

  @impl true
  def handle_info(:connect, state) do
    state =
      with {:ok, state} <- start_rtsp_connection(state),
           {:ok, state} <- get_rtsp_description(state),
           {:ok, state} <- setup_rtsp_connection(state),
           {:ok, state} <- prepare_source(state) do
        state
      else
        {:error, reason, state} -> handle_rtsp_error(reason, state)
      end

    {:noreply, state}
  end

  @impl true
  def handle_info(:source_ready, state) do
    state =
      case play(state) do
        {:ok, state} ->
          %{state | keep_alive_timer: start_keep_alive_timer(state)}

        {:error, reason, state} ->
          handle_rtsp_error(reason, state)
      end

    {:noreply, state}
  end

  @impl true
  def handle_info(:keep_alive, state) do
    {:noreply, keep_alive(state)}
  end

  @impl true
  def handle_info(message, state) do
    Membrane.Logger.warning("received unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    RTSP.close(state.rtsp_session)
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_call({:transfer_rtsp_socket_control, new_controller}, _from, state) do
    {:reply, RTSP.transfer_socket_control(state.rtsp_session, new_controller), state}
  end

  @spec start_rtsp_connection(State.t()) :: connection_establishment_phase_return()
  defp start_rtsp_connection(state) do
    case RTSP.start_link(state.stream_uri,
           response_timeout: Membrane.Time.as_milliseconds(state.timeout, :round)
         ) do
      {:ok, session} ->
        {:ok, %{state | rtsp_session: session}}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  @spec get_rtsp_description(State.t()) :: connection_establishment_phase_return()
  defp get_rtsp_description(%{rtsp_session: rtsp_session} = state, retry \\ true) do
    Membrane.Logger.debug("ConnectionManager: Getting RTSP description")

    case RTSP.describe(rtsp_session, @content_type_header) do
      {:ok, %{status: 200} = response} ->
        tracks = get_tracks(response, state.allowed_media_types)
        {:ok, %{state | tracks: tracks}}

      {:ok, %{status: 401}} ->
        if retry, do: get_rtsp_description(state, false), else: {:error, :unauthorized, state}

      _result ->
        {:error, :getting_rtsp_description_failed, state}
    end
  end

  @spec setup_rtsp_connection(State.t()) :: connection_establishment_phase_return()
  defp setup_rtsp_connection(%{transport: :tcp} = state) do
    case setup_rtsp_connection_with_tcp(state.rtsp_session, state.tracks) do
      {:ok, tracks} -> {:ok, %{state | tracks: tracks}}
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp setup_rtsp_connection(%{transport: {:udp, min_port, max_port}} = state) do
    case setup_rtsp_connection_with_udp(state.rtsp_session, min_port, max_port, state.tracks) do
      {:ok, tracks} -> {:ok, %{state | tracks: tracks}}
      {:error, reason} -> {:error, reason, state}
    end
  end

  @spec prepare_source(State.t()) :: connection_establishment_phase_return()
  defp prepare_source(state) do
    notify_parent(%{tracks: state.tracks}, state)

    {:ok, state}
  end

  @spec play(State.t()) :: connection_establishment_phase_return()
  defp play(%{rtsp_session: rtsp_session, transport: {:udp, _, _}} = state) do
    Membrane.Logger.debug("ConnectionManager: Setting RTSP on play mode")

    case RTSP.play(rtsp_session) do
      {:ok, %{status: 200}} -> {:ok, state}
      _error -> {:error, :play_rtsp_failed, state}
    end
  end

  defp play(%{rtsp_session: rtsp_session, transport: :tcp} = state) do
    Membrane.Logger.debug("ConnectionManager: Setting RTSP on play mode")

    RTSP.play_no_response(rtsp_session)
    {:ok, state}
  end

  @spec keep_alive(State.t()) :: State.t()
  defp keep_alive(state) do
    Membrane.Logger.debug("Send GET_PARAMETER to keep session alive")
    RTSP.get_parameter_no_response(state.rtsp_session)

    %{state | keep_alive_timer: start_keep_alive_timer(state)}
  end

  @spec start_keep_alive_timer(State.t()) :: reference()
  defp start_keep_alive_timer(%{keep_alive_interval: interval}) do
    Process.send_after(self(), :keep_alive, interval |> Membrane.Time.as_milliseconds(:round))
  end

  @spec setup_rtsp_connection_with_tcp(RTSP.t(), [track()]) ::
          {:ok, tracks :: [track()]} | {:error, reason :: term()}
  defp setup_rtsp_connection_with_tcp(rtsp_session, tracks) do
    socket = RTSP.get_socket(rtsp_session)

    tracks
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {track, idx}, {:ok, set_up_tracks} ->
      transport_header =
        [{"Transport", "RTP/AVP/TCP;unicast;interleaved=#{idx * 2}-#{idx * 2 + 1}"}]

      case RTSP.setup(rtsp_session, track.control_path, transport_header) do
        {:ok, %{status: 200}} ->
          {:cont, {:ok, [%{track | transport: {:tcp, socket}} | set_up_tracks]}}

        error ->
          Membrane.Logger.debug(
            "ConnectionManager: Setting up RTSP connection failed: #{inspect(error)}"
          )

          {:halt, {:error, :setting_up_rtsp_connection_failed}}
      end
    end)
  end

  @spec setup_rtsp_connection_with_udp(
          RTSP.t(),
          :inet.port_number(),
          :inet.port_number(),
          [track()],
          [track()]
        ) :: {:ok, tracks :: [track()]} | {:error, reason :: term()}
  defp setup_rtsp_connection_with_udp(
         rtsp_session,
         port,
         max_port,
         tracks,
         set_up_tracks \\ []
       )

  defp setup_rtsp_connection_with_udp(_rtsp_session, _port, _max_port, [], set_up_tracks) do
    {:ok, Enum.reverse(set_up_tracks)}
  end

  defp setup_rtsp_connection_with_udp(_rtsp_session, max_port, max_port, _tracks, _set_up_tracks) do
    # when current port is equal to max_port the range is already exceeded, because port + 1 is also required.
    {:error, :port_range_exceeded}
  end

  defp setup_rtsp_connection_with_udp(rtsp_session, port, max_port, tracks, set_up_tracks) do
    if port_taken?(port) or port_taken?(port + 1) do
      setup_rtsp_connection_with_udp(rtsp_session, port + 1, max_port, tracks, set_up_tracks)
    else
      transport_header = [{"Transport", "RTP/AVP/UDP;unicast;client_port=#{port}-#{port + 1}"}]
      [track | rest_tracks] = tracks

      case RTSP.setup(rtsp_session, track.control_path, transport_header) do
        {:ok, %{status: 200}} ->
          set_up_tracks = [%{track | transport: {:udp, port, port + 1}} | set_up_tracks]

          setup_rtsp_connection_with_udp(
            rtsp_session,
            port + 2,
            max_port,
            rest_tracks,
            set_up_tracks
          )

        _other ->
          {:error, :setup_failed}
      end
    end
  end

  @spec port_taken?(:inet.port_number()) :: boolean()
  defp port_taken?(port) do
    case :gen_udp.open(port, reuseaddr: true) do
      {:ok, socket} ->
        :inet.close(socket)
        false

      _error ->
        true
    end
  end

  @spec handle_rtsp_error(term(), State.t()) :: no_return()
  defp handle_rtsp_error(reason, state) do
    Membrane.Logger.error("could not connect to RTSP server due to: #{inspect(reason)}")
    if state.rtsp_session != nil, do: RTSP.close(state.rtsp_session)

    raise "RTSP connection failed, reason: #{inspect(reason)}"
  end

  @spec notify_parent(term(), State.t()) :: State.t()
  defp notify_parent(msg, state) do
    send(state.parent_pid, msg)
    state
  end

  @spec get_tracks(RTSP.Response.t(), media_types()) :: [track()]
  defp get_tracks(%{body: %ExSDP{media: media_list}}, stream_types) do
    media_list
    |> Enum.filter(&(&1.type in stream_types))
    |> Enum.map(fn media ->
      %{
        control_path: get_attribute(media, "control", ""),
        type: media.type,
        rtpmap: get_attribute(media, ExSDP.Attribute.RTPMapping),
        fmtp: get_attribute(media, ExSDP.Attribute.FMTP),
        transport: nil
      }
    end)
  end

  @spec get_attribute(ExSDP.Media.t(), ExSDP.Attribute.key(), default) ::
          ExSDP.Attribute.t() | default
        when default: var
  defp get_attribute(video_attributes, attribute, default \\ nil) do
    case ExSDP.get_attribute(video_attributes, attribute) do
      {^attribute, value} -> value
      %^attribute{} = value -> value
      _other -> default
    end
  end
end
