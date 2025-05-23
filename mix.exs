defmodule Membrane.RTSP.Plugin.Mixfile do
  use Mix.Project

  @version "0.7.1"
  @github_url "https://github.com/gBillal/membrane_rtsp_plugin"

  def project do
    [
      app: :membrane_rtsp_plugin,
      version: @version,
      elixir: "~> 1.13",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: dialyzer(),

      # hex
      description: "Plugin to simplify connecting to RTSP servers",
      package: package(),

      # docs
      name: "Membrane RTSP plugin",
      source_url: @github_url,
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: []
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:membrane_core, "~> 1.1"},
      {:membrane_rtsp, "~> 0.10.0"},
      {:membrane_rtp_plugin, "~> 0.31.0"},
      {:membrane_rtp_h264_plugin, "~> 0.20.0"},
      {:membrane_rtp_h265_plugin, "~> 0.5.2"},
      {:membrane_rtp_aac_plugin, "~> 0.9.1"},
      {:membrane_rtp_opus_plugin, "~> 0.10.0"},
      {:membrane_tcp_plugin, "~> 0.6.0"},
      {:membrane_h26x_plugin, "~> 0.10.0"},
      {:membrane_udp_plugin, "~> 0.14.0"},
      {:membrane_aac_plugin, "~> 0.19.0"},
      {:ex_sdp, "~> 1.1"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:dialyxir, ">= 0.0.0", only: :dev, runtime: false},
      {:credo, ">= 0.0.0", only: :dev, runtime: false},
      {:mimic, "~> 1.7", only: :test},
      {:membrane_file_plugin, "~> 0.17.0", only: :test}
    ]
  end

  defp dialyzer() do
    opts = [
      flags: [:error_handling]
    ]

    if System.get_env("CI") == "true" do
      # Store PLTs in cacheable directory for CI
      [plt_local_path: "priv/plts", plt_core_path: "priv/plts"] ++ opts
    else
      opts
    end
  end

  defp package do
    [
      maintainers: ["Membrane Team"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @github_url
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "LICENSE"],
      formatters: ["html"],
      source_ref: "v#{@version}",
      nest_modules_by_prefix: [Membrane.RTSP]
    ]
  end
end
