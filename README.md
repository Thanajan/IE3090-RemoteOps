# RemoteOps - IE3090 (IT24102064)

| Item | Value |
|---|---|
| Registration number | IT24102064 |
| Agent port | 7000 + 2410 = **9410** |
| Source files | agent_064.c, controller_064.c, Makefile_064 |
| SID tag | last four digits 2064 reversed = **SID:4602** |
| Auth token | **OPS-2064** |
| Log file | remoteops_IT24102064.log |
| Storage path | ./agentfiles/IT24102064/<filename> |
| ZIP | IE3090_IT24102064.zip |

## Build
    make -f Makefile_064

## Run
    ./agent_064                      # terminal 1 (listens on 9410)
    ./controller_064 127.0.0.1 9410  # terminal 2..n

Controller commands: `AUTH OPS-2064`, `SYSINFO`, `LISTPROC`, `EXEC DATE|UPTIME|DISKFREE|HOSTNAME|WHOAMI`,
`PUT <localpath>`, `GET <name>` (saved to ./downloads), `MONITOR START <udp_port>`, `MONITOR STOP`, `QUIT`.

## Design summary
- Concurrency: one pthread per connection (+ one monitor thread per session while MONITOR is on).
- Framing: per-session receive buffer; read_line() handles partial/multiple lines; PUT/GET move exactly <filesize> bytes.
- UDP monitor: every 2 seconds, `SYSINFO <load> <mem_mb> <uptime> SID:4602`.
- Error codes: 001 AUTH_FAILED, 002 COMMAND_NOT_ALLOWED, 003 NOT_AUTHENTICATED, 004 FILE_TOO_LARGE (>50MB),
  005 FILE_NOT_FOUND, 006 BAD_REQUEST, 007 INVALID_FILENAME, 008 MONITOR_ALREADY_ACTIVE,
  009 MONITOR_NOT_ACTIVE, 010 INTERNAL_ERROR, 011 UNKNOWN_COMMAND.
- Assumptions: connection closed after 3 failed AUTH attempts; QUIT before AUTH is rejected (spec: all commands
  rejected until AUTH); filenames restricted to [A-Za-z0-9._-] (blocks path traversal); EXEC uses fixed command strings only.
