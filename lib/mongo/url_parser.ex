defmodule Mongo.UrlParser do
  @moduledoc """
    Mongo connection URL parsing util

    [See](https://docs.mongodb.com/manual/reference/connection-string/#connections-connection-options) for the complete list of options.

  """

  require Logger

  @mongo_url_regex ~r/^mongodb(?<srv>\+srv)?:\/\/(?:(?<username>[^:]+):(?<password>[^@]+)@)?(?<seeds>[^\/\?]+)(?:\/(?<database>[^?]*)?(?:\?(?<options>(?:[^\s=]+=[^\s&]*)+))?)?$/

  if Code.ensure_loaded?(:ezstd) do
    @compressors ["zstd", "zlib"]
  else
    @compressors ["zlib"]
  end

  # https://docs.mongodb.com/manual/reference/connection-string/#connections-connection-options
  @mongo_options %{
    # Path options
    "username" => :string,
    "password" => :string,
    "database" => :string,
    # Query options
    "replicaSet" => :string,
    "directConnection" => ["true", "false"],
    "ssl" => ["true", "false"],
    "connectTimeoutMS" => :number,
    "socketTimeoutMS" => :number,
    "maxPoolSize" => :number,
    "minPoolSize" => :number,
    "maxIdleTimeMS" => :number,
    "waitQueueMultiple" => :number,
    "waitQueueTimeoutMS" => :number,
    "w" => :number_or_string,
    "wtimeoutMS" => :number,
    "journal" => ["true", "false"],
    "readConcernLevel" => ["local", "majority", "linearizable", "available"],
    "readPreference" => [
      "primary",
      "primaryPreferred",
      "secondary",
      "secondaryPreferred",
      "nearest"
    ],
    "maxStalenessSeconds" => :number,
    "readPreferenceTags" => :string,
    "authSource" => :string,
    "authMechanism" => ["SCRAM-SHA-1", "MONGODB-CR", "MONGODB-X509", "GSSAPI", "PLAIN"],
    "gssapiServiceName" => :string,
    "localThresholdMS" => :number,
    "serverSelectionTimeoutMS" => :number,
    "serverSelectionTryOnce" => ["true", "false"],
    "heartbeatFrequencyMS" => :number,
    "retryWrites" => ["true", "false"],
    "tls" => ["true", "false"],
    "compressors" => @compressors,
    "uuidRepresentation" => ["standard", "csharpLegacy", "javaLegacy", "pythonLegacy"],
    # Elixir Driver options
    "type" => ["unknown", "single", "replicaSetNoPrimary", "sharded"]
  }

  @driver_option_map %{
    max_pool_size: :pool_size,
    replica_set: :set_name,
    w_timeout: :wtimeout
  }

  defp parse_option_value(_key, ""), do: nil

  defp parse_option_value("compressors", values) do
    values
    |> String.split(",")
    |> Enum.filter(fn compressor -> compressor in @compressors end)
  end

  defp parse_option_value(key, value) do
    case @mongo_options[key] do
      :number ->
        String.to_integer(value)

      :string ->
        value

      :number_or_string ->
        case Integer.parse(value) do
          {num, ""} ->
            num

          _string ->
            value
        end

      enum when is_list(enum) ->
        if Enum.member?(enum, value) do
          value
          |> Macro.underscore()
          |> String.to_atom()
        end

      _other ->
        nil
    end
  end

  defp add_option([key, value], opts), do: add_option({key, value}, opts)

  defp add_option({key, value}, opts) do
    case parse_option_value(key, value) do
      nil ->
        opts

      value ->
        key =
          key
          |> Macro.underscore()
          |> String.to_atom()

        value = decode_percent(key, value)

        Keyword.put(opts, @driver_option_map[key] || key, value)
    end
  end

  defp add_option(_other, acc), do: acc

  defp decode_percent(:username, value), do: URI.decode_www_form(value)
  defp decode_percent(:password, value), do: URI.decode_www_form(value)
  defp decode_percent(:auth_source, value), do: URI.decode_www_form(value)
  defp decode_percent(_other, value), do: value

  defp parse_query_options(opts, %{"options" => options}) when is_binary(options) do
    options
    |> String.split("&")
    |> Enum.map(fn option -> String.split(option, "=") end)
    |> Enum.reduce(opts, &add_option/2)
  end

  defp parse_query_options(opts, _frags), do: opts

  defp parse_seeds(opts, %{"seeds" => seeds}) do
    Keyword.put(opts, :seeds, String.split(seeds, ","))
  end

  defp parse_seeds(opts, _frags), do: opts

  defp resolve_srv_url(%{"seeds" => url, "srv" => srv, "options" => orig_options} = frags)
       when is_bitstring(url) and srv == "+srv" do
    # Fix for windows only
    with {:win32, _} <- :os.type() do
      :inet_db.add_ns({4, 2, 2, 1})
    end

    with url_char <- String.to_charlist(url),
         {:ok, {_, _, _, _, _, srv_record}} <-
           :inet_res.getbyname(~c"_mongodb._tcp." ++ url_char, :srv),
         {:ok, host} <- get_host_srv(srv_record),
         {:ok, txt_record} <- resolve_txt_record(url_char),
         txt <- build_params(orig_options, txt_record) do
      frags
      |> Map.put("seeds", host)
      |> Map.put("options", txt)
    else
      err -> err
    end
  end

  defp resolve_srv_url(frags), do: frags

  defp build_params(orig_options, nil) do
    "#{orig_options}&ssl=true"
  end

  defp build_params(orig_options, txt_record) do
    "#{orig_options}&#{txt_record}&ssl=true"
  end

  defp resolve_txt_record(url_char) do
    case :inet_res.lookup(url_char, :in, :txt) do
      [[txt_record] | _] ->
        {:ok, txt_record}

      _other ->
        {:ok, nil}
    end
  end

  @spec get_host_srv([{term, term, term, term}]) :: {:ok, String.t()}
  defp get_host_srv(srv) when is_list(srv) do
    hosts = Enum.map_join(srv, ",", fn {_, _, port, host} -> "#{host}:#{port}" end)

    {:ok, hosts}
  end

  defp hide_password(opts) do
    case Keyword.get(opts, :password) do
      nil ->
        opts

      value ->
        ## start GenServer and put id
        with {:ok, pid} <- Mongo.PasswordSafe.start_link(),
             :ok <- Mongo.PasswordSafe.set_password(pid, value) do
          opts
          |> Keyword.put(:password, "*****")
          |> Keyword.put(:pw_safe, pid)
        end
    end
  end

  @spec parse_url(Keyword.t()) :: Keyword.t()
  def parse_url(opts) when is_list(opts) do
    with {url, opts} when is_binary(url) <- Keyword.pop(opts, :url),
         frags when frags != nil <- Regex.named_captures(@mongo_url_regex, url),
         frags <- resolve_srv_url(frags),
         opts <- parse_seeds(opts, frags),
         opts <- parse_query_options(opts, frags),
         opts <- process_read_preferences(opts),
         # Parse fixed parameters (database, username & password) & merge them with query options
         opts <- Enum.reduce(frags, opts, &add_option/2) do
      opts
    else
      _other -> opts
    end
    |> hide_password()
  end

  def parse_url(opts), do: opts

  defp process_read_preferences(opts) do
    opts =
      case Keyword.get(opts, :read_preference) do
        nil ->
          opts

        mode when is_atom(mode) ->
          read_preference =
            %{mode: mode}
            |> extend_read_preference_tags(opts)
            |> extend_max_staleness_ms(opts)

          Keyword.put(opts, :read_preference, read_preference)

        _other ->
          opts
      end

    Keyword.drop(opts, [:read_preference_tags, :max_staleness_seconds])
  end

  defp extend_read_preference_tags(read_preference, opts) do
    case Keyword.get(opts, :read_preference_tags, []) |> parse_tags() do
      [] ->
        read_preference

      tags ->
        Map.put(read_preference, :tags, Keyword.new(tags))
    end
  end

  defp extend_max_staleness_ms(read_preference, opts) do
    case Keyword.get(opts, :max_staleness_seconds) do
      nil ->
        read_preference

      max_staleness_seconds ->
        Map.put(read_preference, :max_staleness_ms, max_staleness_seconds * 1_000)
    end
  end

  defp parse_tags([]) do
    []
  end

  defp parse_tags(tags) do
    tags
    |> String.split(",")
    |> Enum.map(fn key_value -> to_tuple(key_value) end)
    |> Enum.reject(fn key_value -> key_value == nil end)
  end

  defp to_tuple(key_value) do
    case String.split(key_value, ":") do
      [key, value] ->
        {String.to_atom(key), value}

      _other ->
        Logger.warning("Unable to parse the read preference tags #{inspect(key_value)}")
        nil
    end
  end
end
