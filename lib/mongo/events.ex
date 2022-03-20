defmodule Mongo.Events do
  @doc false

  require Logger

  def notify(event, topic \\ :topology) do
    Registry.dispatch(:events_registry, topic, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:broadcast, topic, event})
    end)
  end

  defmodule CommandStartedEvent do
    @moduledoc false
    defstruct [
      ## Returns the command.
      :command,
      ## Returns the database name.
      :database_name,
      ## Returns the command name.
      :command_name,
      ## Returns the driver generated request id.
      :request_id,
      ## Returns the driver generated operation id. This is used to link events together such
      :operation_id,
      ## as bulk write operations. OPTIONAL.
      ## Returns the connection id for the command. For languages that do not have this,
      :connection_id
      ## this MUST return the driver equivalent which MUST include the server address and port.
      ## The name of this field is flexible to match the object that is returned from the drive
    ]
  end

  defmodule CommandSucceededEvent do
    @moduledoc false

    defstruct [
      ## Returns the execution time of the event in the highest possible resolution for the platform.
      :duration,
      ## The calculated value MUST be the time to send the message and receive the reply from the server
      ## and MAY include BSON serialization and/or deserialization. The name can imply the units in which the
      ## value is returned, i.e. durationMS, durationNanos.
      ## Returns the command reply.
      :reply,
      ## Returns the command name.
      :command_name,
      ## Returns the driver generated request id.
      :request_id,
      ## Returns the driver generated operation id. This is used to link events together such
      :operation_id,
      ## as bulk write operations. OPTIONAL.
      ## Returns the connection id for the command. For languages that do not have this,
      :connection_id
      ## this MUST return the driver equivalent which MUST include the server address and port.
      ## The name of this field is flexible to match the object that is returned from the driver.
    ]
  end

  defmodule CommandFailedEvent do
    @moduledoc false

    defstruct [
      ## Returns the execution time of the event in the highest possible resolution for the platform.
      :duration,
      ## The calculated value MUST be the time to send the message and receive the reply from the server
      ## and MAY include BSON serialization and/or deserialization. The name can imply the units in which the
      ## value is returned, i.e. durationMS, durationNanos.
      ## Returns the command name.
      :command_name,
      ## Returns the failure. Based on the language, this SHOULD be a message string, exception
      :failure,
      ## object, or error document.
      ## Returns the driver generated request id.
      :request_id,
      ## Returns the driver generated operation id. This is used to link events together such
      :operation_id,
      ## as bulk write operations. OPTIONAL.
      ## Returns the connection id for the command. For languages that do not have this,
      :connection_id
      ## this MUST return the driver equivalent which MUST include the server address and port.
      ## The name of this field is flexible to match the object that is returned from the driver.
    ]
  end

  defmodule MoreToComeEvent do
    @moduledoc false
    defstruct [
      ## Returns the command.
      :command,
      ## Returns the command name.
      :command_name
    ]
  end

  defmodule RetryReadEvent do
    @moduledoc false
    defstruct [
      ## Returns the command.
      :command,
      ## Returns the command name.
      :command_name
    ]
  end

  defmodule RetryWriteEvent do
    @moduledoc false

    defstruct [
      ## Returns the command.
      :command,
      ## Returns the command name.
      :command_name
    ]
  end

  ##
  #
  defmodule ServerSelectionEmptyEvent do
    @moduledoc false
    defstruct [:action, :cmd_type, :topology, :opts]
  end

  # Published when server description changes, but does NOT include changes to
  # the RTT
  defmodule ServerDescriptionChangedEvent do
    @moduledoc false
    defstruct [:address, :topology_pid, :previous_description, :new_description]
  end

  # Published when server is initialized
  defmodule ServerOpeningEvent do
    @moduledoc false
    defstruct [:address, :topology_pid]
  end

  # Published when server is closed
  defmodule ServerClosedEvent do
    @moduledoc false
    defstruct [:address, :topology_pid]
  end

  # Published when topology description changes
  defmodule TopologyDescriptionChangedEvent do
    @moduledoc false
    defstruct [:topology_pid, :previous_description, :new_description]
  end

  # Published when topology is initialized
  defmodule TopologyOpeningEvent do
    @moduledoc false
    defstruct [:topology_pid]
  end

  # Published when topology is closed
  defmodule TopologyClosedEvent do
    @moduledoc false
    defstruct [:topology_pid]
  end

  # Fired when the server monitor’s ismaster command is started - immediately
  # before the ismaster command is serialized into raw BSON and written to the
  # socket.
  defmodule ServerHeartbeatStartedEvent do
    @moduledoc false
    defstruct [:connection_pid]
  end

  # Fired when the server monitor’s ismaster succeeds
  defmodule ServerHeartbeatSucceededEvent do
    @moduledoc false
    defstruct [:duration, :reply, :connection_pid]
  end

  # Fired when the server monitor’s ismaster fails, either with an “ok: 0” or
  # a socket exception.
  defmodule ServerHeartbeatFailedEvent do
    @moduledoc false
    defstruct [:duration, :failure, :connection_pid]
  end
end
