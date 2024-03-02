defmodule Membrane.RTSP.Source.ConnectionManager do
  @moduledoc false

  use GenServer

  require Membrane.Logger

  require Logger
  alias Membrane.RTSP

  @content_type_header [{"accept", "application/sdp"}]
  @response_timeout :timer.seconds(15)
  @keep_alive_interval :timer.seconds(15)

  @type media_types :: [:video | :audio | :application]
  @type connection_opts :: %{stream_uri: binary(), allowed_media_types: media_types()}

  @spec start_link(connection_opts()) :: GenServer.on_start()
  def start_link(options) do
    GenServer.start_link(__MODULE__, Map.put(options, :parent_pid, self()))
  end

  @impl true
  def init(options) do
    state =
      Map.merge(options, %{
        rtsp_session: nil,
        tracks: [],
        status: :init,
        keep_alive_timer: nil
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
        notify_parent(
          state,
          {:tracks, Enum.map(state.tracks, &elem(&1, 1)), RTSP.get_transport(state.rtsp_session)}
        )

        %{keep_alive(state) | status: :connected}
      else
        {:error, reason, state} ->
          Logger.error("could not connect to RTSP server due to: #{inspect(reason)}")
          if is_pid(state.rtsp_session), do: RTSP.close(state.rtsp_session)

          state = notify_parent(state, {:connection_failed, reason}) |> retry()
          %{state | status: :failed}
      end

    {:noreply, state}
  end

  @impl true
  def handle_info(:keep_alive, state) do
    RTSP.get_parameter_no_response(state.rtsp_session)
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

        _other ->
          state
      end

    {:noreply, %{state | rtsp_session: nil, status: :init}}
  end

  def handle_info(message, state) do
    Membrane.Logger.warning("received unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

  defp start_rtsp_connection(state) do
    case RTSP.start(state.stream_uri, RTSP.Transport.TCPSocket,
           response_timeout: @response_timeout
         ) do
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
    |> Enum.reduce_while({:ok, state}, fn {{control_path, _track}, idx}, acc ->
      transport_header = [
        {"Transport", "RTP/AVP/TCP;unicast;interleaved=#{idx * 2}-#{idx * 2 + 1}"}
      ]

      case RTSP.setup(rtsp_session, control_path, transport_header) do
        {:ok, %{status: 200}} ->
          {:cont, acc}

        result ->
          Membrane.Logger.debug(
            "ConnectionManager: Setting up RTSP connection failed: #{inspect(result)}"
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

  defp keep_alive(state) do
    Membrane.Logger.debug("Send GET_PARAMETER to keep session alive")
    RTSP.get_parameter_no_response(state.rtsp_session)
    %{state | keep_alive_timer: Process.send_after(self(), :keep_alive, @keep_alive_interval)}
  end

  defp cancel_keep_alive(state) do
    :timer.cancel(state.keep_alive_timer)
    %{state | keep_alive_timer: nil}
  end

  # notify the parent only once on successive failures
  defp notify_parent(%{status: :failed} = state, _msg), do: state

  defp notify_parent(state, msg) do
    send(state.parent_pid, msg)
    state
  end

  defp retry(state) do
    Process.send_after(self(), :connect, :timer.seconds(3))
    state
  end

  defp get_tracks(%{body: %ExSDP{media: media_list}}, stream_types) do
    media_list
    |> Enum.filter(&(&1.type in stream_types))
    |> Enum.map(fn media ->
      {get_attribute(media, "control", ""),
       %{
         type: media.type,
         rtpmap: get_attribute(media, ExSDP.Attribute.RTPMapping),
         fmtp: get_attribute(media, ExSDP.Attribute.FMTP)
       }}
    end)
  end

  defp get_attribute(video_attributes, attribute, default \\ nil) do
    case ExSDP.get_attribute(video_attributes, attribute) do
      {^attribute, value} -> value
      %^attribute{} = value -> value
      _other -> default
    end
  end
end
