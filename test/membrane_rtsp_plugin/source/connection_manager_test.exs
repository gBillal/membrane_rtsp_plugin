defmodule Membrane.RTSP.Source.ConnectionManagerTest do
  use ExUnit.Case, async: true
  use Mimic.DSL

  alias Membrane.RTSP
  alias Membrane.RTSP.Response
  alias Membrane.RTSP.Source.ConnectionManager

  @stream_uri "rtsp://localhost:8554/mystream"
  @allowed_media_types [:video, :audio, :application]
  @sdp """
  v=0
  o=- 0 0 IN IP4 127.0.0.1
  s=MyVideoSession
  t=0 0
  m=video 0 RTP/AVP 96
  a=rtpmap:96 H264/90000
  a=fmtp:96 profile-level-id=42e01f;packetization-mode=1
  m=audio 0 RTP/AVP 97
  a=rtpmap:97 OPUS/48000/2
  a=fmtp:97 minptime=10; useinbandfec=1
  m=application 0 RTP/AVP 98
  """

  setup do
    opts = %{
      stream_uri: @stream_uri,
      allowed_media_types: @allowed_media_types,
      parent_pid: self()
    }

    %{opts: opts}
  end

  test "initialize state", %{opts: opts} do
    assert {:ok,
            %{
              stream_uri: @stream_uri,
              allowed_media_types: @allowed_media_types,
              rtsp_session: nil,
              tracks: [],
              keep_alive_timer: nil,
              status: :init,
              parent_pid: self()
            }} ==
             ConnectionManager.init(opts)
  end

  test "successful connection", %{opts: opts} do
    pid = :c.pid(0, 1, 1)

    expect(RTSP.start(@stream_uri, _transport, _options), do: {:ok, pid})

    expect RTSP.describe(^pid, [{"accept", "application/sdp"}]) do
      {:ok, %Response{Response.new(200) | body: ExSDP.parse!(@sdp)}}
    end

    expect(RTSP.setup(^pid, "", _headers), [num_calls: 3], do: {:ok, Response.new(200)})
    expect(RTSP.play(^pid), do: {:ok, Response.new(200)})
    expect(RTSP.get_transport(^pid), do: %{})

    assert {:ok, state} = ConnectionManager.init(opts)

    assert {:noreply, state} = ConnectionManager.handle_info(:connect, state)
    assert state.status == :connected
    assert state.rtsp_session == pid

    assert_received {:tracks, tracks, %{}}
    assert length(tracks) == 3
  end

  test "failed connection", %{opts: opts} do
    pid = :c.pid(0, 1, 1)

    expect(RTSP.start(@stream_uri, _transport, _options), do: {:error, :econnrefused})
    expect(RTSP.start(@stream_uri, _transport, _options), do: {:ok, pid})

    expect(RTSP.describe(^pid, [{"accept", "application/sdp"}]), do: {:ok, Response.new(401)})
    expect(RTSP.describe(^pid, [{"accept", "application/sdp"}]), do: {:ok, Response.new(404)})

    assert {:ok, state} = ConnectionManager.init(opts)
    assert {:noreply, %{status: :failed} = state} = ConnectionManager.handle_info(:connect, state)
    assert_received {:connection_failed, :econnrefused}

    assert {:noreply, %{status: :failed}} = ConnectionManager.handle_info(:connect, state)
    refute_received {:connection_failed, :setting_up_sdp_connection_failed}
  end
end
