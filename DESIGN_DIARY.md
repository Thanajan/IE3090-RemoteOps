## 6 Oct 2026
Today I set up my CentOS VM and installed gcc, make and git. My first run failed with "No such file or directory" because I was in the wrong folder.
 I fixed it with cd. After that, I tested AUTH, SYSINFO, EXEC, PUT/GET and MONITOR, and they worked. Pushing to GitHub gave a 403 error because,
 my token had no repo permission, so I created a new classic token.

Design decision: I chose one thread per client because each connection can use a simple blocking loop, and threads share the log file easily. It is enogh
for five clients.
