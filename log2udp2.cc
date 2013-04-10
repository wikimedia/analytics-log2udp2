#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <poll.h>
#include <getopt.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>


static option options[] = {
    { "number",	no_argument,		0, 'n' },
    { "prefix",	required_argument,	0, 'p' },
    { "ignore",	no_argument,		0, 'i' },
    { "timeout",	required_argument,	0, 't' },
    { "delim",	required_argument,	0, 'd' },
    { 0, 0, 0, 0 }
};

static const char*	    line_prefix = 0;
static int              line_prefix_len = 0;
static const char*	    line_delim = "\t";
static int              line_delim_len = 1;
static bool		        numbering = false;
static bool		        ignore_net_errs = false;
static unsigned long	line_number = 0;
static int		        timeout = -1;

static void usage(const char* exename)
{
    fprintf(stderr,
            "Usage: %s [OPTION]... HOST PORT\n"
            "Send standard input to UDP port PORT at HOST\n\n"
            "Mandatory arguments to long options are mandatory for short options too.\n"
            "  -n, --number           number outgoing lines\n"
            "  -p, --prefix=PREFIX    prepend \"PREFIX\" to outgoing lines\n"
            "  -i, --ignore           ignore network errors (fire-and-forget)\n"
            "  -t, --timeout=MSEC     wait MSEC miliseconds at most when merging\n"
            "                         consecutive lines in a packet (-1 to wait\n"
            "                         indefinitely, which is the default)\n"
            "  -d, --delim=DELIM      separate line number, prefix and line text\n"
            "                         with DELIM, defaulting to a tab.\n",
            exename);
}

// Our output buffer: we keep a quasi-circular data queue outbuf_size long
// (quasi insofar as we roll over when we reach 64k from the end to keep packet
// data contiguous), and a circular packet queue pktbuf_size long.  Both need
// to be power-of-two in size.

// the "currently built" packet lies beyond outbuf_head and is outbuf_len long.

static const size_t	outbuf_size = 1024*1024;
static char		    outbuf[outbuf_size];
static int		    outbuf_tail = 0;
static int		    outbuf_head = 0;
static size_t		outbuf_len = 0;

static const size_t	pktbuf_size = 1024;
static struct {
    off_t			data;
    size_t			len;
}			    pktbuf[pktbuf_size];
static int		pktbuf_tail = 0;
static int		pktbuf_head = 0;


static bool hasroom(void)
{
    if((pktbuf_head+1) % pktbuf_size == pktbuf_tail)
        return false;

    if(outbuf_head < outbuf_tail)
        return outbuf_tail-outbuf_head >= 65536;
    if(outbuf_head+65536 < outbuf_size)
        return true;
    if(outbuf_tail >= 65536)
        return true;
    return false;
}

// This function "wraps up" the output packet we are currently
// building and makes it available for output.
static void endpacket(void)
{
    if(outbuf_len > 0) {
        pktbuf[pktbuf_head].data = outbuf_head;
        pktbuf[pktbuf_head].len = outbuf_len;
        pktbuf_head = (pktbuf_head+1) % pktbuf_size;
        outbuf_head += outbuf_len;
        outbuf_len = 0;
        if(outbuf_head+65536 >= outbuf_size)
            outbuf_head = 0;
    }
}

static void addline(const char* buf, size_t len)
{
    char hdr[line_delim_len*2 + line_prefix_len + 32];

    while(len && buf[len-1]=='\r')
        --len;
    if(!len)
        return;

    int hdr_len = 0;

    if(numbering) {
        sprintf(hdr, "%ld%s", line_number++, line_delim);
        hdr_len += strlen(hdr);
    }
    if(line_prefix) {
        sprintf(hdr+hdr_len, "%s%s", line_prefix, line_delim);
        hdr_len += line_prefix_len + line_delim_len;
    }

    if(len+hdr_len > 65534)
        len = 65534-hdr_len;

    if(outbuf_len && (outbuf_len+hdr_len+len) > 1491)
        endpacket();

    char* cp_ptr = outbuf+outbuf_head;

    if(hdr_len) {
        memcpy(cp_ptr+outbuf_len, hdr, hdr_len);
        outbuf_len += hdr_len;
    }
    memcpy(cp_ptr+outbuf_len, buf, len);
    outbuf_len += len;
    cp_ptr[outbuf_len++] = '\n';

    if(outbuf_len >= 1491-hdr_len)
        endpacket();
}


static const size_t	insize = 1024*1024;
static char		    input_buf[insize];
static size_t		input_len = 0;

int main(int argc, char** argv, char** envp)
{
    bool	flushing = false;
    pollfd	fds[2];

    for(;;) {
        int	o = getopt_long(argc, argv, "np:it:d:", options, 0);

        if(o<0)
            break;

        switch(o) {
            case 'n':
                numbering = true;
                break;
            case 'p':
                line_prefix_len = strlen(optarg);
                line_prefix = new char[line_prefix_len+1];
                strcpy((char*)line_prefix, optarg);
                break;
            case 't':
                timeout = atoi(optarg);
                break;
            case 'i':
                ignore_net_errs = true;
                break;
            case 'd':
                line_delim_len = strlen(optarg);
                line_delim = new char[line_delim_len+1];
                strcpy((char*)line_delim, optarg);
                break;
            case ':':
            case '?':
                usage(argv[0]);
                return 1;
        }
    }

    if(optind != argc-2) {
        usage(argv[0]);
        return 1;
    }

    // minimal sanity check
    if(line_prefix_len+line_delim_len > 512) {
        fprintf(stderr, "%s: combined prefix and delimiter length too long\n", argv[0]);
        return 1;
    }

    int sock = -1;

    addrinfo	hints = { 0, 0, SOCK_DGRAM, 0, 0, 0, 0, 0 };
    addrinfo	*addr;

    if(int err = getaddrinfo(argv[optind], argv[optind+1], &hints, &addr)) {
        fprintf(stderr, "%s: %s(%s): %s\n", argv[0], argv[optind], argv[optind+1], gai_strerror(err));
        return 1;
    }

    for(addrinfo* a=addr; a; a=a->ai_next) {
        if((sock = socket(a->ai_family, a->ai_socktype, a->ai_protocol)) >= 0) {
            if(!connect(sock, a->ai_addr, a->ai_addrlen)) {
                break;
            }
            close(sock);
        }
        if(!a->ai_next) {
            perror("connect()");
            return 1;
        }
    }

    freeaddrinfo(addr);

    fds[0].fd = STDIN_FILENO;
    fds[1].fd = sock;

    for(;;) {
        int pollrv = 0;

        // Part of the trick here is that we only add to the poll() call
        // the FDs we know we can proceed (i.e. the buffers are not full)

        if(flushing) {
            if(pktbuf_tail == pktbuf_head)
                break;
            fds[1].events = POLLOUT;
            pollrv = poll(&fds[1], 1, -1);
        } else {
            fds[0].events = 0;
            fds[1].events = 0;

            if(input_len < insize)
                fds[0].events = POLLIN;
            if(pktbuf_tail != pktbuf_head)
                fds[1].events = POLLOUT;
            pollrv = poll(fds, 2, outbuf_len? timeout: -1);
        }

        if(pollrv < 0) {
            perror("poll()");
            if(errno == EINTR)
                continue;
            return 2;
        }

        if(pollrv == 0) {
            endpacket();
        }

        if(fds[0].revents & (POLLERR|POLLHUP|POLLNVAL)) {
            endpacket();
            flushing = true;
        } else if(fds[0].revents & POLLIN) {
            int readlen = read(fds[0].fd, input_buf+input_len, insize-input_len);
            if(readlen < 0) {
                // Shouldn't happen
                perror("read()");
                endpacket();
                flushing = true;
            }
            if(readlen == 0) {
                endpacket();
                flushing = true;
            }
            if(readlen > 0) {
                input_len += readlen;
                int	input_ptr = 0;
                while(input_ptr<input_len && hasroom()) {
                    int lend = input_ptr;
                    while(lend<input_len && input_buf[lend]!='\n')
                        lend++;
                    if(lend >= input_len)
                        break;
                    addline(input_buf+input_ptr, lend-input_ptr);
                    input_ptr = lend+1;
                }
                if(input_ptr && input_ptr<input_len) {
                    memmove(input_buf, input_buf+input_ptr, input_len-input_ptr);
                    input_len -= input_ptr;
                } else
                    input_len = 0;
            }
        }

        if(fds[1].revents & (POLLERR|POLLHUP|POLLNVAL)) {
            // Linux can give ECONNREFUSED upon receiving some ICMP
            // packets on the same UDP triple (rather than just cause
            // an error on the send).  This works around that case.
            if(errno!=ECONNREFUSED || !ignore_net_errs) {
                fprintf(stderr, "%s: error condition on socket\n", argv[0]);
                return 2;
            }
        }
        if(fds[1].revents & POLLOUT) {
            while(pktbuf_head != pktbuf_tail) {
                int sendlen = send(fds[1].fd, outbuf+pktbuf[pktbuf_tail].data, pktbuf[pktbuf_tail].len, MSG_DONTWAIT|MSG_NOSIGNAL);

                if(sendlen < 0) {
                    if(errno==EAGAIN || errno==EWOULDBLOCK)
                        break;
                    if(ignore_net_errs) {
                        sendlen = pktbuf[pktbuf_tail].len;
                    } else {
                        perror("send()");
                        return 2;
                    }
                }
                if(sendlen == 0)
                    break;

                pktbuf_tail = (pktbuf_tail+1) & (pktbuf_size-1);
                if(pktbuf_tail == pktbuf_head)
                    outbuf_tail = outbuf_head;
                else
                    outbuf_tail = pktbuf[pktbuf_tail].data;
            }
        }
    }

    return 0;
}

