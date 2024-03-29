defmodule Membrane.RTSP.Source.Transport.TCPWrapper do
  @moduledoc false

  use Membrane.RTSP.Transport

  alias Membrane.RTSP.Transport.TCPSocket

  @impl true
  def init(url, options) do
    with {:ok, tcp_socket} <- TCPSocket.init(url, options) do
      if pid = options[:controlling_process] do
        :gen_tcp.controlling_process(tcp_socket, pid)
      end

      {:ok, tcp_socket}
    end
  end

  @impl true
  defdelegate execute(request, socket, options), to: TCPSocket

  @impl true
  defdelegate handle_info(msg, state), to: TCPSocket

  @impl true
  defdelegate close(state), to: TCPSocket
end
