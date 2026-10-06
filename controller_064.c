/*
 * RemoteOps Controller (client) - IE3090 Assignment, IT24102064
 * Usage: ./controller_064 [host] [port]     (default 127.0.0.1 9410)
 * Commands typed at the prompt are the protocol commands, except:
 *   PUT <localpath>       uploads the file (name = basename of path)
 *   GET <name>            saves to ./downloads/<name>
 *   MONITOR START <port>  also opens a local UDP socket on <port>
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <libgen.h>

#define DEFAULT_PORT 9410
#define RBUF 65536

static int sock = -1, udp = -1;
static char rbuf[RBUF]; static size_t rlen = 0;

static int send_all(const void *b, size_t n)
{
    const char *p = b;
    while (n > 0) {
        ssize_t r = send(sock, p, n, MSG_NOSIGNAL);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        p += r; n -= (size_t)r;
    }
    return 0;
}

static int read_line(char *out, size_t outsz)
{
    for (;;) {
        char *nl = memchr(rbuf, '\n', rlen);
        if (nl) {
            size_t n = (size_t)(nl - rbuf), c = n < outsz - 1 ? n : outsz - 1;
            memcpy(out, rbuf, c); out[c] = '\0';
            rlen -= n + 1; memmove(rbuf, nl + 1, rlen);
            return 1;
        }
        if (rlen >= RBUF) return -1;
        ssize_t r = recv(sock, rbuf + rlen, RBUF - rlen, 0);
        if (r == 0) return 0;
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        rlen += (size_t)r;
    }
}

static ssize_t read_some(char *dst, size_t max)
{
    if (rlen > 0) {
        size_t n = rlen < max ? rlen : max;
        memcpy(dst, rbuf, n); rlen -= n; memmove(rbuf, rbuf + n, rlen);
        return (ssize_t)n;
    }
    for (;;) {
        ssize_t r = recv(sock, dst, max, 0);
        if (r < 0 && errno == EINTR) continue;
        return r;
    }
}

static void close_udp(void) { if (udp >= 0) { close(udp); udp = -1; } }

static int open_udp(int port)
{
    close_udp();
    udp = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp < 0) return -1;
    int y = 1; setsockopt(udp, SOL_SOCKET, SO_REUSEADDR, &y, sizeof y);
    struct sockaddr_in a; memset(&a, 0, sizeof a);
    a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((uint16_t)port);
    if (bind(udp, (struct sockaddr *)&a, sizeof a) < 0) { perror("udp bind"); close_udp(); return -1; }
    return 0;
}

static int do_put(const char *path)
{
    FILE *f = fopen(path, "rb");
    struct stat st;
    if (!f || stat(path, &st) != 0) { printf("Cannot open local file '%s'\n", path); if (f) fclose(f); return 0; }
    char tmp[512]; strncpy(tmp, path, sizeof tmp - 1); tmp[sizeof tmp - 1] = 0;
    char hdr[600];
    int n = snprintf(hdr, sizeof hdr, "PUT %s %ld\n", basename(tmp), (long)st.st_size);
    if (send_all(hdr, (size_t)n) < 0) { fclose(f); return -1; }
    char buf[8192]; size_t r;
    while ((r = fread(buf, 1, sizeof buf, f)) > 0)
        if (send_all(buf, r) < 0) { fclose(f); return -1; }
    fclose(f);
    return 1;
}

static int do_get_body(const char *resp)
{
    char ok[16], tag[16], name[256]; long size;
    if (sscanf(resp, "OK FILE_SEND %255s %ld", name, &size) != 2) return 0;
    (void)ok; (void)tag;
    mkdir("downloads", 0755);
    char path[300]; snprintf(path, sizeof path, "downloads/%s", name);
    FILE *f = fopen(path, "wb");
    if (!f) { perror("fopen"); return -1; }
    char buf[8192]; long rem = size;
    while (rem > 0) {
        ssize_t r = read_some(buf, rem < (long)sizeof buf ? (size_t)rem : sizeof buf);
        if (r <= 0) { fclose(f); return -1; }
        fwrite(buf, 1, (size_t)r, f); rem -= r;
    }
    fclose(f);
    printf("[saved %ld bytes to %s]\n", size, path);
    return 1;
}

int main(int argc, char **argv)
{
    const char *host = argc > 1 ? argv[1] : "127.0.0.1";
    int port = argc > 2 ? atoi(argv[2]) : DEFAULT_PORT;

    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof hints); hints.ai_family = AF_INET; hints.ai_socktype = SOCK_STREAM;
    char ps[16]; snprintf(ps, sizeof ps, "%d", port);
    if (getaddrinfo(host, ps, &hints, &res) != 0) { fprintf(stderr, "cannot resolve %s\n", host); return 1; }
    sock = socket(res->ai_family, res->ai_socktype, 0);
    if (sock < 0 || connect(sock, res->ai_addr, res->ai_addrlen) < 0) { perror("connect"); return 1; }
    freeaddrinfo(res);
    printf("Connected to %s:%d. Type AUTH <token> first. (Ctrl-D to exit)\n", host, port);

    char line[1024], resp[RBUF];
    for (;;) {
        printf("remoteops> "); fflush(stdout);
        fd_set rs; FD_ZERO(&rs); FD_SET(0, &rs); int mx = 0;
        if (udp >= 0) { FD_SET(udp, &rs); mx = udp; }
        if (select(mx + 1, &rs, NULL, NULL, NULL) < 0) { if (errno == EINTR) continue; break; }
        if (udp >= 0 && FD_ISSET(udp, &rs)) {
            char d[512]; ssize_t n = recvfrom(udp, d, sizeof d - 1, 0, NULL, NULL);
            if (n > 0) { d[n] = 0; printf("\n[UDP MONITOR] %s\n", d); }
            if (!FD_ISSET(0, &rs)) continue;
        }
        if (!FD_ISSET(0, &rs)) continue;
        if (!fgets(line, sizeof line, stdin)) break;
        line[strcspn(line, "\r\n")] = 0;
        if (!line[0]) continue;

        int is_get = !strncmp(line, "GET ", 4);
        int is_stop = !strcmp(line, "MONITOR STOP");
        int mon_port = 0;
        if (!strncmp(line, "MONITOR START ", 14)) {
            mon_port = atoi(line + 14);
            if (mon_port < 1 || mon_port > 65535 || open_udp(mon_port) < 0) { if (mon_port < 1 || mon_port > 65535) printf("bad UDP port\n"); continue; }
        }

        if (!strncmp(line, "PUT ", 4)) {
            int r = do_put(line + 4);
            if (r < 0) { printf("connection lost\n"); break; }
            if (r == 0) continue;
        } else {
            char out[1100]; int n = snprintf(out, sizeof out, "%s\n", line);
            if (send_all(out, (size_t)n) < 0) { printf("connection lost\n"); break; }
        }

        int r = read_line(resp, sizeof resp);
        if (r <= 0) { printf("connection closed by Agent\n"); break; }
        printf("%s\n", resp);
        if (is_get && !strncmp(resp, "OK FILE_SEND", 12) && do_get_body(resp) < 0) { printf("download failed\n"); break; }
        if (mon_port && strncmp(resp, "OK", 2)) close_udp();
        if (is_stop && !strncmp(resp, "OK", 2)) close_udp();
        if (!strcmp(line, "QUIT")) break;
    }
    close_udp(); close(sock);
    return 0;
}
