defmodule Membrane.RTSP.Source.ConnectionManager do
  @moduledoc false

  use GenServer

  require Membrane.Logger

  alias Membrane.RTSP
  alias Membrane.RTSP.Source.Transport.TCPWrapper

  @content_type_header [{"accept", "application/sdp"}]

  @udp_min_port 5000
  @udp_max_port 65_000

  @base_back_off_in_ms 10
  @max_back_off_in_ms :timer.minutes(2)

  @type media_types :: [:video | :audio | :application]
  @type connection_opts :: %{stream_uri: binary(), allowed_media_types: media_types()}

  @spec start_link(connection_opts()) :: GenServer.on_start()
  def start_link(options) do
    GenServer.start_link(__MODULE__, Map.put(options, :parent_pid, self()))
  end

  @spec stop(pid()) :: :ok
  def stop(server) do
    GenServer.call(server, :stop)
  end

  @impl true
  def init(options) do
    state =
      Map.merge(options, %{
        rtsp_session: nil,
        tracks: [],
        status: :init,
        keep_alive_timer: nil,
        reconnect_attempt: 0
      })

    Process.send_after(self(), :connect, 0)
    {:ok, state}
  end

  @impl true
  def handle_info(:connect, state) do
    state =
      with {:ok, state} <- start_rtsp_connection(state),
           {:ok, state} <- get_rtsp_description(state),
           {:ok, state} <- setup_rtsp_connection(state),
           :ok <- play(state) do
        %{state | status: :connected, reconnect_attempt: 0}
        |> notify_parent({:tracks, Map.values(state.tracks)})
        |> keep_alive()
      else
        {:error, reason, state} ->
          Membrane.Logger.error("could not connect to RTSP server due to: #{inspect(reason)}")
          if is_pid(state.rtsp_session), do: RTSP.close(state.rtsp_session)

          state = notify_parent(state, {:connection_failed, reason}) |> retry()
          %{state | status: :failed}
      end

    {:noreply, state}
  end

  @impl true
  def handle_info(:keep_alive, state) do
    {:noreply, keep_alive(state)}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, %{rtsp_session: pid} = state) do
    state =
      case state.status do
        :connected ->
          state
          |> notify_parent({:connection_failed, reason})
          |> cancel_keep_alive()
          |> retry()
          |> then(&%{&1 | status: :failed})

        _other ->
          state
      end

    {:noreply, %{state | rtsp_session: nil}}
  end

  @impl true
  def handle_info(message, state) do
    Membrane.Logger.warning("received unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    Membrane.RTSP.close(state.rtsp_session)
    {:stop, :normal, :ok, state}
  end

  defp start_rtsp_connection(state) do
    options = [response_timeout: state.timeout, controlling_process: state.parent_pid]

    case RTSP.start(state.stream_uri, TCPWrapper, options) do
      {:ok, session} ->
        Process.monitor(session)
        {:ok, %{state | rtsp_session: session}}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

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

  defp setup_rtsp_connection(%{rtsp_session: rtsp_session} = state) do
    Membrane.Logger.debug("ConnectionManager: Setting up RTSP connection")

    state.tracks
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, state}, fn {{control_path, _track}, idx}, {:ok, state} ->
      with {:ok, transport, transport_header} <- build_transport_header(state, idx),
           {:ok, %{status: 200}} <- RTSP.setup(rtsp_session, control_path, transport_header) do
        tracks = Map.update!(state.tracks, control_path, &%{&1 | transport: transport})
        {:cont, {:ok, %{state | tracks: tracks}}}
      else
        error ->
          Membrane.Logger.debug(
            "ConnectionManager: Setting up RTSP connection failed: #{inspect(error)}"
          )

          {:halt, {:error, :setting_up_sdp_connection_failed, state}}
      end
    end)
  end

  defp play(%{rtsp_session: rtsp_session} = state) do
    Membrane.Logger.debug("ConnectionManager: Setting RTSP on play mode")

    case RTSP.play(rtsp_session) do
      {:ok, %{status: 200}} -> :ok
      _error -> {:error, :play_rtsp_failed, state}
    end
  end

  defp build_transport_header(%{transport: :tcp} = state, media_id) do
    {:ok, RTSP.get_transport(state.rtsp_session),
     [{"Transport", "RTP/AVP/TCP;unicast;interleaved=#{media_id * 2}-#{media_id * 2 + 1}"}]}
  end

  defp build_transport_header(%{transport: :udp}, _media_id) do
    @udp_min_port..@udp_max_port//2
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_free_port}, fn rtp_port, acc ->
      if free_port?(rtp_port) and free_port?(rtp_port + 1) do
        {:halt,
         {:ok, {rtp_port, rtp_port + 1},
          [{"Transport", "RTP/AVP;unicast;client_port=#{rtp_port}-#{rtp_port + 1}"}]}}
      else
        {:cont, acc}
      end
    end)
  end

  defp free_port?(port) do
    case :gen_udp.open(port, reuseaddr: true) do
      {:ok, socket} ->
        :inet.close(socket)
        true

      _error ->
        false
    end
  end

  defp keep_alive(state) do
    Membrane.Logger.debug("Send GET_PARAMETER to keep session alive")
    RTSP.get_parameter_no_response(state.rtsp_session)

    %{
      state
      | keep_alive_timer: Process.send_after(self(), :keep_alive, state.keep_alive_interval)
    }
  end

  defp cancel_keep_alive(state) do
    Process.cancel_timer(state.keep_alive_timer)
    %{state | keep_alive_timer: nil}
  end

  # notify the parent only once on successive failures
  defp notify_parent(%{status: :failed} = state, _msg), do: state

  defp notify_parent(state, msg) do
    send(state.parent_pid, msg)
    state
  end

  defp retry(%{reconnect_attempt: attempt} = state) do
    delay =
      :math.pow(2, attempt)
      |> Kernel.*(@base_back_off_in_ms)
      |> min(@max_back_off_in_ms)
      |> trunc()

    Membrane.Logger.info("retry connection in #{delay} ms")
    Process.send_after(self(), :connect, delay)
    %{state | reconnect_attempt: attempt + 1}
  end

  defp get_tracks(%{body: %ExSDP{media: media_list}}, stream_types) do
    media_list
    |> Enum.filter(&(&1.type in stream_types))
    |> Enum.map(fn media ->
      {get_attribute(media, "control", ""),
       %{
         type: media.type,
         rtpmap: get_attribute(media, ExSDP.Attribute.RTPMapping),
         fmtp: get_attribute(media, ExSDP.Attribute.FMTP),
         transport: nil
       }}
    end)
    |> Map.new()
  end

  defp get_attribute(video_attributes, attribute, default \\ nil) do
    case ExSDP.get_attribute(video_attributes, attribute) do
      {^attribute, value} -> value
      %^attribute{} = value -> value
      _other -> default
    end
  end
end
