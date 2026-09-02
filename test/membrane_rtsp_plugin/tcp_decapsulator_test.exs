defmodule Membrane.RTSP.DecapsulatorTest do
  use ExUnit.Case

  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec

  alias Membrane.RTSP.TCP.Decapsulator
  alias Membrane.Testing.{Pipeline, Sink, Source}

  @header_length 4

  defp encapsulate_rtp_packets(rtp_packets) do
    Enum.map(rtp_packets, &<<"$", 0, byte_size(&1)::size(16), &1::binary>>)
  end

  defp create_tcp_segments(encapsulated_rtp_packets, tcp_segments_lengths) do
    assert Enum.sum(tcp_segments_lengths) ==
             Enum.sum(Enum.map(encapsulated_rtp_packets, &byte_size(&1)))

    encaplsulated_rtp_packets_binary = Enum.join(encapsulated_rtp_packets)

    {tcp_segments, _length} =
      Enum.map_reduce(tcp_segments_lengths, 0, fn len, pos ->
        {:binary.part(encaplsulated_rtp_packets_binary, pos, len), pos + len}
      end)

    tcp_segments
  end

  defp perform_standard_test(rtp_packets_lengths, tcp_segments_lengths) do
    rtp_packets = Enum.map(rtp_packets_lengths, &<<0::size(&1)-unit(8)>>)

    tcp_segments =
      rtp_packets |> encapsulate_rtp_packets() |> create_tcp_segments(tcp_segments_lengths)

    pipeline =
      Pipeline.start_link_supervised!(
        spec:
          child(:source, %Source{
            output: tcp_segments
          })
          |> child(:decapsulator, %Decapsulator{rtsp_session: self()})
          |> child(:sink, Sink)
      )

    Enum.each(rtp_packets, fn packet ->
      assert_sink_buffer(pipeline, :sink, %Membrane.Buffer{payload: ^packet})
    end)

    Pipeline.terminate(pipeline)
  end

  describe "RTSP Decapsulator decapsulates correctly" do
    test "when one tcp segment is one rtp packet" do
      rtp_packets_lengths = 10..20
      tcp_segments_lengths = Enum.map(rtp_packets_lengths, &(&1 + @header_length))

      perform_standard_test(rtp_packets_lengths, tcp_segments_lengths)
    end

    test "when there are multiple (3) rtp packets in one tcp segment" do
      rtp_packets_lengths = 10..40

      tcp_segments_lengths =
        rtp_packets_lengths
        |> Enum.chunk_every(3)
        |> Enum.map(&(Enum.sum(&1) + length(&1) * @header_length))

      perform_standard_test(rtp_packets_lengths, tcp_segments_lengths)
    end

    test "when rtp packets are spread across multiple (3) tcp segments" do
      rtp_packets_lengths = 11..41//3

      tcp_segments_lengths =
        Enum.flat_map(rtp_packets_lengths, fn len ->
          tcp_segment_base_length = div(len + @header_length, 3)
          [tcp_segment_base_length - 1, tcp_segment_base_length, tcp_segment_base_length + 1]
        end)

      perform_standard_test(rtp_packets_lengths, tcp_segments_lengths)
    end
  end

  describe "RTSP Decapsulator recovers from a desynced TCP stream" do
    test "resyncs past leading junk to the next frame marker" do
      rtp_packets = [<<1, 2, 3, 4, 5>>, <<6, 7, 8, 9, 10>>]
      junk = <<0xA4, 0x0F, 0x72, 0x1F, 0xF1>>
      segment = junk <> Enum.join(encapsulate_rtp_packets(rtp_packets))

      pipeline =
        Pipeline.start_link_supervised!(
          spec:
            child(:source, %Source{output: [segment]})
            |> child(:decapsulator, %Decapsulator{rtsp_session: self()})
            |> child(:sink, Sink)
        )

      Enum.each(rtp_packets, fn packet ->
        assert_sink_buffer(pipeline, :sink, %Membrane.Buffer{payload: ^packet})
      end)

      Pipeline.terminate(pipeline)
    end

    test "drops a junk-only segment and recovers on the next valid frame" do
      rtp_packet = <<11, 12, 13, 14, 15>>
      junk = <<0xA4, 0x0F, 0x72, 0x1F, 0xF1, 0x00, 0x99>>
      [valid] = encapsulate_rtp_packets([rtp_packet])

      pipeline =
        Pipeline.start_link_supervised!(
          spec:
            child(:source, %Source{output: [junk, valid]})
            |> child(:decapsulator, %Decapsulator{rtsp_session: self()})
            |> child(:sink, Sink)
        )

      assert_sink_buffer(pipeline, :sink, %Membrane.Buffer{payload: ^rtp_packet})

      Pipeline.terminate(pipeline)
    end

    test "recovers from a real desynced RTCP frame captured from a Sony SNC camera in production" do
      # Real bytes that crashed get_complete_packets/3: an interleaved RTCP
      # RR+SDES frame ("SNC-G6 H264") whose framing had desynced the TCP parser. Old code hit
      # a FunctionClauseError here; the fix resyncs to the "$" at offset 34 and emits the
      # 32-byte frame that follows.
      crash_prefix =
        <<0, 32, 128, 201, 0, 1, 40, 68, 20, 241, 129, 202, 0, 5, 40, 68, 20, 241, 1, 11, 83,
          78, 67, 45, 71, 54, 32, 72, 50, 54, 52, 0, 0, 0, 36, 1, 0, 32, 128, 201, 0, 1, 40,
          68, 20, 241, 129, 202, 0, 5, 40, 68, 20, 241, 1, 11, 83, 78, 67, 45, 71, 54, 32, 72,
          50, 54, 52, 0, 0, 0>>

      <<_desynced::binary-size(34), "$", 1, 32::size(16), expected::binary-size(32)>> =
        crash_prefix

      pipeline =
        Pipeline.start_link_supervised!(
          spec:
            child(:source, %Source{output: [crash_prefix]})
            |> child(:decapsulator, %Decapsulator{rtsp_session: self()})
            |> child(:sink, Sink)
        )

      assert_sink_buffer(pipeline, :sink, %Membrane.Buffer{payload: ^expected})
      assert_end_of_stream(pipeline, :sink)

      Pipeline.terminate(pipeline)
    end
  end
end
