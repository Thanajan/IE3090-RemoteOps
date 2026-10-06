/*
 * RemoteOps Agent (server) - IE3090 Assignment
 * Registration number : IT24102064
 * Port                : 7000 + 2410 = 9410
 * SID tag             : SID:4602   (last four digits 2064 reversed)
 * Auth token          : OPS-2064
 * Log file            : remoteops_IT24102064.log
 * Storage path        : ./agentfiles/IT24102064/<filename>
 *
 * Concurrency model: one POSIX thread per Controller connection.
 * Each session also has an optional monitor thread for UDP stats.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define REG_NO       "IT24102064"
#define SID          "4602"
#define AUTH_TOKEN   "OPS-2064"
#define AGENT_PORT   9410
#define STORE_DIR    "./agentfiles/IT24102064"
#define LOG_FILE     "remoteops_IT24102064.log"

#define MON_INTERVAL_SEC 2            /* UDP datagram every 2 seconds */
#define RBUF_SIZE        2048         /* max line length incl. '\n'   */
#define MAX_FILE_SIZE    (50L*1024*1024)   /* 50 MB -> ERR 004        */
#define DRAIN_LIMIT      (1024L*1024*1024) /* beyond this, just close */
#define MAX_AUTH_FAILS   3

/* ---------------- logging ---------------- */
static FILE *logf_fp;
static pthread_mutex_t log_mu = PTHREAD_MUTEX_INITIALIZER;

static void log_msg(const char *fmt, ...)
{
    char ts[32]; time_t now = time(NULL); struct tm tm;
    localtime_r(&now, &tm);
    strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tm);
    pthread_mutex_lock(&log_mu);
    fprintf(logf_fp, "[%s] ", ts);
    va_list ap; va_start(ap, fmt); vfprintf(logf_fp, fmt, ap); va_end(ap);
    fputc('\n', logf_fp);
    fflush(logf_fp);
    pthread_mutex_unlock(&log_mu);
}

/* ---------------- session ---------------- */
typedef struct {
    int fd;
    struct sockaddr_in peer;
    char peer_str[64];
    int id;
    int authed;
    char rbuf[RBUF_SIZE];   /* receive buffer for framing */
    size_t rlen;
    /* UDP monitoring */
    volatile int mon_active;
    int mon_port;
    pthread_t mon_thread;
} session_t;

static volatile sig_atomic_t running = 1;
static int next_session_id = 1;
static pthread_mutex_t id_mu = PTHREAD_MUTEX_INITIALIZER;

/* ---------------- system info helpers ---------------- */
static void get_sysinfo(char *out, size_t n)
{
    double load = 0; long up = 0; long total = 0, avail = 0;
    FILE *f = fopen("/proc/loadavg", "r");
    if (f) { if (fscanf(f, "%lf", &load) != 1) load = 0; fclose(f); }
    f = fopen("/proc/uptime", "r");
    if (f) { double u; if (fscanf(f, "%lf", &u) == 1) up = (long)u; fclose(f); }
    f = fopen("/proc/meminfo", "r");
    if (f) {
        char line[256];
        while (fgets(line, sizeof line, f)) {
            sscanf(line, "MemTotal: %ld kB", &total);
            sscanf(line, "MemAvailable: %ld kB", &avail);
        }
        fclose(f);
    }
    long used_mb = (total - avail) / 1024;
    snprintf(out, n, "%.2f %ld %ld", load, used_mb, up);
}

/* ---------------- I/O helpers ---------------- */
static int send_all(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        p += n; len -= (size_t)n;
    }
    return 0;
}

/* Send one response line; always appends " SID:<sid>\n". */
static int reply(session_t *s, const char *fmt, ...)
{
    char body[16384], line[16500];
    va_list ap; va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);
    int n = snprintf(line, sizeof line, "%s SID:%s\n", body, SID);
    if (n >= (int)sizeof line) n = sizeof line - 1;
    return send_all(s->fd, line, (size_t)n);
}

/* Returns 1 = line read, 0 = EOF, -1 = error, -2 = line too long.
 * Handles partial lines and multiple lines per recv(). */
static int read_line(session_t *s, char *out, size_t outsz)
{
    for (;;) {
        char *nl = memchr(s->rbuf, '\n', s->rlen);
        if (nl) {
            size_t n = (size_t)(nl - s->rbuf);
            size_t c = n < outsz - 1 ? n : outsz - 1;
            memcpy(out, s->rbuf, c);
            out[c] = '\0';
            if (c > 0 && out[c-1] == '\r') out[c-1] = '\0';
            s->rlen -= n + 1;
            memmove(s->rbuf, nl + 1, s->rlen);
            return 1;
        }
        if (s->rlen >= RBUF_SIZE) return -2;
        ssize_t r = recv(s->fd, s->rbuf + s->rlen, RBUF_SIZE - s->rlen, 0);
        if (r == 0) return 0;
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        s->rlen += (size_t)r;
    }
}

/* Read up to max raw bytes: first from leftover buffer, then socket. */
static ssize_t read_some(session_t *s, char *dst, size_t max)
{
    if (s->rlen > 0) {
        size_t n = s->rlen < max ? s->rlen : max;
        memcpy(dst, s->rbuf, n);
        s->rlen -= n;
        memmove(s->rbuf, s->rbuf + n, s->rlen);
        return (ssize_t)n;
    }
    for (;;) {
        ssize_t r = recv(s->fd, dst, max, 0);
        if (r < 0 && errno == EINTR) continue;
        return r;
    }
}

/* ---------------- UDP monitor thread ---------------- */
static void *monitor_thread(void *arg)
{
    session_t *s = arg;
    int u = socket(AF_INET, SOCK_DGRAM, 0);
    if (u < 0) return NULL;
    struct sockaddr_in dst = s->peer;
    dst.sin_port = htons((uint16_t)s->mon_port);
    while (s->mon_active) {
        char info[128], msg[200];
        get_sysinfo(info, sizeof info);
        int n = snprintf(msg, sizeof msg, "SYSINFO %s SID:%s", info, SID);
        sendto(u, msg, (size_t)n, 0, (struct sockaddr *)&dst, sizeof dst);
        for (int i = 0; i < MON_INTERVAL_SEC * 10 && s->mon_active; i++)
            usleep(100000);   /* sleep in slices so STOP is fast */
    }
    close(u);
    return NULL;
}

static void stop_monitor(session_t *s)
{
    if (s->mon_active) {
        s->mon_active = 0;
        pthread_join(s->mon_thread, NULL);
        log_msg("session %d (%s): UDP monitoring stopped", s->id, s->peer_str);
    }
}

/* ---------------- command helpers ---------------- */
static int valid_filename(const char *f)
{
    size_t n = strlen(f);
    if (n == 0 || n > 100 || f[0] == '.') return 0;
    for (size_t i = 0; i < n; i++) {
        char c = f[i];
        if (!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||
              c=='.'||c=='_'||c=='-')) return 0;
    }
    return 1;
}

/* Run a FIXED command string (never built from user input). */
static void run_fixed(const char *cmd, char *out, size_t n)
{
    out[0] = '\0';
    FILE *p = popen(cmd, "r");
    if (!p) { snprintf(out, n, "error"); return; }
    size_t len = 0; int c;
    while ((c = fgetc(p)) != EOF && len < n - 1)
        out[len++] = (c == '\n' || c == '\r') ? ' ' : (char)c;
    out[len] = '\0';
    while (len > 0 && out[len-1] == ' ') out[--len] = '\0';
    pclose(p);
}

static void cmd_listproc(session_t *s)
{
    char out[8192]; size_t len = 0;
    FILE *p = popen("ps -eo pid,comm --no-headers", "r");
    if (!p) { reply(s, "ERR 010 INTERNAL_ERROR"); return; }
    char line[256];
    while (fgets(line, sizeof line, p) && len < sizeof out - 300) {
        int pid; char name[200];
        if (sscanf(line, "%d %199s", &pid, name) == 2)
            len += (size_t)snprintf(out + len, sizeof out - len, "%s%d:%s",
                                    len ? "," : "", pid, name);
    }
    pclose(p);
    out[len] = '\0';
    reply(s, "OK PROCS %s", out);
}

static void cmd_exec(session_t *s, const char *name, const char *extra)
{
    const char *cmd = NULL;
    if (!extra) {
        if      (!strcmp(name, "DATE"))     cmd = "date";
        else if (!strcmp(name, "UPTIME"))   cmd = "uptime";
        else if (!strcmp(name, "DISKFREE")) cmd = "df -h /";
        else if (!strcmp(name, "HOSTNAME")) cmd = "hostname";
        else if (!strcmp(name, "WHOAMI"))   cmd = "whoami";
    }
    if (!cmd) {
        log_msg("session %d: EXEC rejected (not whitelisted)", s->id);
        reply(s, "ERR 002 COMMAND_NOT_ALLOWED");
        return;
    }
    char out[1024];
    run_fixed(cmd, out, sizeof out);
    log_msg("session %d: EXEC %s", s->id, name);
    reply(s, "OK EXEC_RESULT %s", out);
}

/* Consume and discard n bytes of an upload we are rejecting. */
static int drain(session_t *s, long n)
{
    char buf[4096];
    while (n > 0) {
        ssize_t r = read_some(s, buf, n < (long)sizeof buf ? (size_t)n : sizeof buf);
        if (r <= 0) return -1;
        n -= r;
    }
    return 0;
}

/* returns -1 if the connection must be dropped */
static int cmd_put(session_t *s, const char *fname, const char *sizestr)
{
    char *end; long size;
    if (!fname || !sizestr) { reply(s, "ERR 006 BAD_REQUEST"); return 0; }
    size = strtol(sizestr, &end, 10);
    if (*end != '\0' || size < 0) { reply(s, "ERR 006 BAD_REQUEST"); return 0; }

    if (size > MAX_FILE_SIZE || !valid_filename(fname)) {
        int toolarge = size > MAX_FILE_SIZE;
        if (size > DRAIN_LIMIT) {
            reply(s, "ERR 004 FILE_TOO_LARGE");
            return -1;
        }
        if (drain(s, size) < 0) return -1;
        log_msg("session %d: PUT %s rejected (%s)", s->id, fname,
                toolarge ? "too large" : "invalid filename");
        if (toolarge) reply(s, "ERR 004 FILE_TOO_LARGE");
        else          reply(s, "ERR 007 INVALID_FILENAME");
        return 0;
    }

    char path[512], tmp[560];
    snprintf(path, sizeof path, "%s/%s", STORE_DIR, fname);
    snprintf(tmp, sizeof tmp, "%s.part", path);
    FILE *f = fopen(tmp, "wb");
    if (!f) {
        if (drain(s, size) < 0) return -1;
        reply(s, "ERR 010 INTERNAL_ERROR");
        return 0;
    }
    long remaining = size; char buf[8192]; int failed = 0;
    while (remaining > 0) {
        ssize_t r = read_some(s, buf, remaining < (long)sizeof buf ? (size_t)remaining : sizeof buf);
        if (r <= 0) { failed = 1; break; }
        fwrite(buf, 1, (size_t)r, f);
        remaining -= r;
    }
    fclose(f);
    if (failed) {
        remove(tmp);
        log_msg("session %d: PUT %s aborted (client disconnected)", s->id, fname);
        return -1;
    }
    rename(tmp, path);
    log_msg("session %d: PUT %s (%ld bytes) stored at %s", s->id, fname, size, path);
    reply(s, "OK FILE_RECEIVED %s", fname);
    return 0;
}

static int cmd_get(session_t *s, const char *fname)
{
    if (!fname || !valid_filename(fname)) { reply(s, "ERR 005 FILE_NOT_FOUND"); return 0; }
    char path[512];
    snprintf(path, sizeof path, "%s/%s", STORE_DIR, fname);
    struct stat st;
    FILE *f = fopen(path, "rb");
    if (!f || stat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
        if (f) fclose(f);
        log_msg("session %d: GET %s -> not found", s->id, fname);
        reply(s, "ERR 005 FILE_NOT_FOUND");
        return 0;
    }
    if (reply(s, "OK FILE_SEND %s %ld", fname, (long)st.st_size) < 0) { fclose(f); return -1; }
    char buf[8192]; size_t n; long sent = 0;
    while ((n = fread(buf, 1, sizeof buf, f)) > 0) {
        if (send_all(s->fd, buf, n) < 0) { fclose(f); return -1; }
        sent += (long)n;
    }
    fclose(f);
    log_msg("session %d: GET %s (%ld bytes) sent", s->id, fname, sent);
    return 0;
}

/* ---------------- per-connection thread ---------------- */
static void *client_thread(void *arg)
{
    session_t *s = arg;
    char line[RBUF_SIZE + 1];
    int auth_fails = 0;

    log_msg("session %d: connection from %s", s->id, s->peer_str);

    for (;;) {
        int r = read_line(s, line, sizeof line);
        if (r == 0)  { log_msg("session %d: client %s disconnected", s->id, s->peer_str); break; }
        if (r == -1) { log_msg("session %d: recv error (%s) - dropping %s", s->id, strerror(errno), s->peer_str); break; }
        if (r == -2) { reply(s, "ERR 006 BAD_REQUEST"); log_msg("session %d: line too long, closing", s->id); break; }
        if (line[0] == '\0') continue;

        /* Tokenise a copy (never log the token itself) */
        char copy[RBUF_SIZE + 1];
        strcpy(copy, line);
        char *save = NULL;
        char *cmd = strtok_r(copy, " ", &save);
        char *a1 = strtok_r(NULL, " ", &save);
        char *a2 = strtok_r(NULL, " ", &save);
        char *a3 = strtok_r(NULL, " ", &save);

        if (!strcmp(cmd, "AUTH")) {
            if (a1 && !a2 && !strcmp(a1, AUTH_TOKEN)) {
                s->authed = 1;
                log_msg("session %d: AUTH success", s->id);
                reply(s, "OK AUTHENTICATED");
            } else {
                log_msg("session %d: AUTH failed (%d)", s->id, ++auth_fails);
                reply(s, "ERR 001 AUTH_FAILED");
                if (auth_fails >= MAX_AUTH_FAILS) {
                    log_msg("session %d: too many auth failures, closing", s->id);
                    break;
                }
            }
            continue;
        }

        if (!s->authed) {
            log_msg("session %d: command '%s' rejected (not authenticated)", s->id, cmd);
            reply(s, "ERR 003 NOT_AUTHENTICATED");
            continue;
        }

        if (!strcmp(cmd, "SYSINFO")) {
            char info[128]; get_sysinfo(info, sizeof info);
            log_msg("session %d: SYSINFO", s->id);
            reply(s, "OK SYSINFO %s", info);
        } else if (!strcmp(cmd, "LISTPROC")) {
            log_msg("session %d: LISTPROC", s->id);
            cmd_listproc(s);
        } else if (!strcmp(cmd, "EXEC")) {
            if (!a1) { reply(s, "ERR 002 COMMAND_NOT_ALLOWED"); }
            else     cmd_exec(s, a1, a2);
        } else if (!strcmp(cmd, "PUT")) {
            if (a3) { reply(s, "ERR 006 BAD_REQUEST"); continue; }
            if (cmd_put(s, a1, a2) < 0) break;
        } else if (!strcmp(cmd, "GET")) {
            if (a2) { reply(s, "ERR 006 BAD_REQUEST"); continue; }
            if (cmd_get(s, a1) < 0) break;
        } else if (!strcmp(cmd, "MONITOR")) {
            if (a1 && !strcmp(a1, "START") && a2 && !a3) {
                int port = atoi(a2);
                if (port < 1 || port > 65535) { reply(s, "ERR 006 BAD_REQUEST"); continue; }
                if (s->mon_active) { reply(s, "ERR 008 MONITOR_ALREADY_ACTIVE"); continue; }
                s->mon_port = port; s->mon_active = 1;
                if (pthread_create(&s->mon_thread, NULL, monitor_thread, s) != 0) {
                    s->mon_active = 0; reply(s, "ERR 010 INTERNAL_ERROR"); continue;
                }
                log_msg("session %d: MONITOR START -> %s:%d every %ds", s->id, inet_ntoa(s->peer.sin_addr), port, MON_INTERVAL_SEC);
                reply(s, "OK MONITOR_STARTED");
            } else if (a1 && !strcmp(a1, "STOP") && !a2) {
                if (!s->mon_active) { reply(s, "ERR 009 MONITOR_NOT_ACTIVE"); continue; }
                stop_monitor(s);
                reply(s, "OK MONITOR_STOPPED");
            } else reply(s, "ERR 006 BAD_REQUEST");
        } else if (!strcmp(cmd, "QUIT")) {
            stop_monitor(s);
            log_msg("session %d: QUIT", s->id);
            reply(s, "OK BYE");
            break;
        } else {
            log_msg("session %d: unknown command '%s'", s->id, cmd);
            reply(s, "ERR 011 UNKNOWN_COMMAND");
        }
    }

    stop_monitor(s);
    close(s->fd);
    log_msg("session %d: closed", s->id);
    free(s);
    return NULL;
}

static void on_signal(int sig) { (void)sig; running = 0; }

int main(void)
{
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa; memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;           /* no SA_RESTART: accept() returns EINTR */
    sigaction(SIGINT, &sa, NULL); sigaction(SIGTERM, &sa, NULL);

    logf_fp = fopen(LOG_FILE, "a");
    if (!logf_fp) { perror("log file"); return 1; }

    mkdir("./agentfiles", 0755);
    mkdir(STORE_DIR, 0755);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return 1; }
    int yes = 1; setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
    struct sockaddr_in addr; memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(AGENT_PORT);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof addr) < 0) { perror("bind"); return 1; }
    if (listen(lfd, 16) < 0) { perror("listen"); return 1; }

    printf("RemoteOps Agent (%s) listening on port %d, SID:%s\n", REG_NO, AGENT_PORT, SID);
    log_msg("Agent started for %s on port %d", REG_NO, AGENT_PORT);

    while (running) {
        struct sockaddr_in cli; socklen_t cl = sizeof cli;
        int fd = accept(lfd, (struct sockaddr *)&cli, &cl);
        if (fd < 0) { if (errno == EINTR) continue; perror("accept"); continue; }
        session_t *s = calloc(1, sizeof *s);
        if (!s) { close(fd); continue; }
        s->fd = fd; s->peer = cli;
        snprintf(s->peer_str, sizeof s->peer_str, "%s:%d", inet_ntoa(cli.sin_addr), ntohs(cli.sin_port));
        pthread_mutex_lock(&id_mu); s->id = next_session_id++; pthread_mutex_unlock(&id_mu);
        pthread_t t;
        if (pthread_create(&t, NULL, client_thread, s) != 0) { close(fd); free(s); continue; }
        pthread_detach(t);
    }
    log_msg("Agent shutting down");
    close(lfd);
    fclose(logf_fp);
    return 0;
}
