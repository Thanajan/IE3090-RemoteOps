Testing Notes - IT24102064
Date: 6 October 2026
Environment: CentOS VM, Agent on port 9410, Controller on 127.0.0.1.

Completed tests:
1. AUTH with the correct token OPS-2064 - accepted and logged.
2. AUTH with a wrong token - rejected, log shows one failed attempt.
3. LISTPROC - returned the running processes.
4. EXEC DATE and EXEC WHOAMI - both executed successfully.
5. EXEC ls - rejected because it is not on the whitelist.
6. PUT and GET of Makefile_064 (321 bytes) - cmp showed the files were identical.
7. MONITOR START 9600 and MONITOR STOP - both recorded in the log.
8. A mistyped command - logged as unknown, and the Agent kept running.
9. ss -tlnp showed agent_064 listening on port 9410.
10. The file was stored in agentfiles/IT24102064/.

Tests still to run:
- Large binary file transfer
- Command split into two parts
- Five simultaneous clients
- Sudden client disconnect
