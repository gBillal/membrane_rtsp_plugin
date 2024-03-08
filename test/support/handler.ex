defmodule Membrane.RTSP.RequestHandler do
  @moduledoc false

  @behaviour Membrane.RTSP.Server.Handler

  require Logger

  alias Membrane.RTSP.Response

  @sdp """
  v=0
  o=- 0 0 IN IP4 127.0.0.1
  s=MyVideoSession
  t=0 0
  m=video 0 RTP/AVP 96
  a=rtpmap:96 H264/90000
  a=fmtp:96 profile-level-id=42e01f;packetization-mode=1
  a=control:/in.h264
  m=video 0 RTP/AVP 97
  a=rtpmap:97 H265/90000
  a=control:/in.hevc
  m=application 0 RTP/AVP 107
  a=control:/in.txt
  a=rtpmap:107 plain/90000
  a=appversion:1.0
  """

  @impl true
  def handle_open_connection(_conn) do
    sources = %{
      H264: %{
        encoding: :H264,
        location: "test/fixtures/in.h264",
        payload_type: 96,
        clock_rate: 90_000
      },
      H265: %{
        encoding: :H265,
        location: "test/fixtures/in.hevc",
        payload_type: 97,
        clock_rate: 90_000
      },
      plain: %{
        encoding: :plain,
        location: "test/fixtures/in.txt",
        payload_type: 107,
        clock_rate: 90_000
      }
    }

    %{
      sources: sources,
      pipeline_pid: nil
    }
  end

  @impl true
  def handle_describe(_req, state) do
    Response.new(200)
    |> Response.with_header("Content-Type", "application/sdp")
    |> Response.with_body(@sdp)
    |> then(&{&1, state})
  end

  @impl true
  def handle_setup(_req, state), do: {Response.new(200), state}

  @impl true
  def handle_play(configured_media_context, state) do
    sources =
      Enum.reduce(configured_media_context, %{}, fn {control_path, config}, sources ->
        source_key =
          cond do
            String.ends_with?(control_path, "/in.h264") -> :H264
            String.ends_with?(control_path, "/in.hevc") -> :H265
            String.ends_with?(control_path, "/in.txt") -> :plain
            true -> nil
          end

        Map.put(sources, source_key, Map.merge(state.sources[source_key], config))
      end)

    {:ok, _sup_pid, pipeline_pid} =
      Membrane.RTSP.ServerPipeline.start_link(Map.values(sources))

    {Response.new(200), %{state | pipeline_pid: pipeline_pid}}
  end

  @impl true
  def handle_pause(state) do
    {Response.new(501), state}
  end

  @impl true
  def handle_teardown(state) do
    {Response.new(200), state}
  end

  @impl true
  def handle_closed_connection(_state), do: :ok
end
