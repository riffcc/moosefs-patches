#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#include "../../mfscommon/md5.h"
#include "mfs_ctrl_proto.h"
/* Userspace-safe MooseFS wire protocol constants */
#include "mfs_wire_defs.h"

#define HELPER_MAX_MSG (sizeof(struct mfs_ctrl_hdr) + MFS_CTRL_MAX_PAYLOAD)
#define HELPER_INFO_STR "mfskmod-helper"
#define HELPER_CS_TIMEOUT_MS 12000
#define HELPER_READ_TIMEOUT_SINGLE_MS 4000
#define HELPER_READ_TIMEOUT_MULTI_MS 1500
#define HELPER_READ_TIMEOUT_SHARED_MS 1000
#define HELPER_READ_TIMEOUT_FANOUT_MS 500
#define HELPER_READ_IO_TIMEOUT_SINGLE_MS 4000
#define HELPER_READ_IO_TIMEOUT_MULTI_MS 2500
#define HELPER_READ_IO_TIMEOUT_FANOUT_MS 2000
#define HELPER_MASTER_TIMEOUT_MS 12000
#define HELPER_MASTER_MAX_SKIPPED_PKTS 128
#define HELPER_READ_CACHE_MAX (64U * 1024U * 1024U)
#define HELPER_READ_PREFETCH_BASE (16U * 1024U * 1024U)
#define HELPER_READ_PREFETCH_MAX (64U * 1024U * 1024U)
#define HELPER_READ_SEQ_TRIGGER (2U * 1024U * 1024U)
#define HELPER_READ_SCOREBOARD_MAX 32
#define HELPER_READ_META_CACHE_MAX 64
#define HELPER_READ_AFFINITY_CACHE_MAX 64
#define HELPER_READ_STREAM_AFFINITY_MAX 32
#define HELPER_READ_FOREGROUND_MAX (4U * 1024U * 1024U)
#define HELPER_READ_WORKERS 4
#define HELPER_READ_JOB_QUEUE_MAX 32
#define HELPER_READ_REPLICA_LOOKAHEAD 4U
#define HELPER_READ_STREAM_BAND_SHIFT 28
#define HELPER_READ_STRIPE_META_MAX 16
#define HELPER_READ_STRIPE_FILL_CREDITS 4U
#define HELPER_READ_PREFETCH_WORKERS 1
#define HELPER_READ_PREFETCH_QUEUE_MAX 2
#define HELPER_READ_PREFETCH_DATA_CACHE_MAX 2
#define HELPER_READ_PREFETCH_WORKER_MAX (8U * 1024U * 1024U)
#define HELPER_GETATTR_CACHE_TTL_MS 5000U
#define HELPER_GETATTR_CACHE_MAX 16
#define VERSION2INT(maj,mid,min) ((maj) * 0x10000U + (mid) * 0x100U + (((maj) > 1) ? ((min) * 2U) : (min)))
#define CHUNKOPFLAG_CANMODTIME 1U
#define CHUNKOPFLAG_CONTINUEOP 2U

struct cs_loc {
	uint32_t ip;
	uint16_t port;
	uint32_t cs_ver;
	uint32_t labelmask;
};

struct chunk_meta {
	uint8_t protocol;
	uint64_t length;
	uint64_t chunkid;
	uint32_t version;
	uint32_t loc_count;
	struct cs_loc locs[16];
};

struct range_span {
	uint32_t start;
	uint32_t end;
};

struct read_meta_cache_entry {
	bool valid;
	bool pending;
	uint32_t inode;
	uint32_t chunk_idx;
	int status;
	struct chunk_meta meta;
	uint64_t stamp;
};

struct read_affinity_entry {
	bool valid;
	uint32_t inode;
	uint32_t preferred_ip;
	uint16_t preferred_port;
	uint64_t stamp;
};

struct read_prefetch_request {
	bool valid;
	uint32_t inode;
	uint32_t chunk_idx;
	uint32_t chunk_off;
	uint32_t fetch_len;
};

struct read_prefetch_data_entry {
	bool valid;
	uint32_t inode;
	uint32_t chunk_idx;
	uint32_t base;
	uint32_t len;
	uint8_t *buf;
	uint32_t cap;
	uint64_t stamp;
};

struct read_stream_affinity_entry {
	bool valid;
	uint32_t inode;
	uint64_t band;
	uint32_t worker_id;
	uint64_t stamp;
};

struct getattr_cache_entry {
	bool valid;
	bool pending;
	uint32_t inode;
	uint32_t uid;
	uint32_t gid;
	uint64_t expires_ms;
	uint64_t stamp;
	struct mfs_wire_attr attr;
};

/*
 * Stripe-plan substrate: sequential readers should converge on one metadata
 * plan per active band instead of rediscovering chunk metadata one request at
 * a time.
 */
struct read_stripe_meta_entry {
	bool valid;
	uint32_t chunk_idx;
	int status;
	struct chunk_meta meta;
};

struct read_state {
	bool meta_valid;
	int meta_status;
	uint32_t inode;
	uint32_t chunk_idx;
	struct chunk_meta meta;
	int cs_fd;
	uint32_t cs_ip;
	uint16_t cs_port;
	uint8_t *cache_buf;
	uint32_t cache_cap;
	uint32_t cache_base;
	uint32_t scoreboard_count;
	struct range_span scoreboard[HELPER_READ_SCOREBOARD_MAX];
	uint32_t prefetch_len;
	uint64_t last_end;
	uint64_t seq_bytes;
	uint32_t preferred_ip;
	uint16_t preferred_port;
	uint64_t stripe_band;
	uint32_t stripe_base_chunk;
	uint32_t stripe_chunk_count;
	struct read_stripe_meta_entry stripe_meta[HELPER_READ_STRIPE_META_MAX];
};

struct read_job {
	bool valid;
	struct mfs_ctrl_hdr hdr;
	uint8_t *payload;
	uint32_t payload_len;
	uint32_t preferred_worker;
};

struct read_worker_arg {
	struct helper_session *session;
	uint32_t worker_id;
};

struct helper_session {
	bool active;
	int master_fd;
	uint32_t next_msgid;
	uint32_t session_id;
	uint32_t master_version;
	uint8_t attr_size;
	char master_host[256];
	uint16_t master_port;
	char subdir[MFS_PATH_MAX + 1];
	pthread_t master_thread;
	pthread_mutex_t master_mu;
	pthread_cond_t master_cv;
	bool master_thread_running;
	bool master_thread_stop;
	bool master_req_pending;
	bool master_req_done;
	uint32_t master_req_type;
	uint8_t *master_req_data;
	uint32_t master_req_len;
	uint32_t master_rsp_type;
	uint8_t *master_rsp_data;
	uint32_t master_rsp_len;
	int master_rsp_status;
	time_t master_last_nop;
	bool read_meta_valid;
	int read_meta_status;
	uint32_t read_inode;
	uint32_t read_chunk_idx;
	struct chunk_meta read_meta;
	int read_cs_fd;
	uint32_t read_cs_ip;
	uint16_t read_cs_port;
	uint8_t *read_cache_buf;
	uint32_t read_cache_cap;
	uint32_t read_cache_base;
	uint32_t read_scoreboard_count;
	struct range_span read_scoreboard[HELPER_READ_SCOREBOARD_MAX];
	uint32_t read_prefetch_len;
	uint64_t read_last_end;
	uint64_t read_seq_bytes;
	pthread_t read_prefetch_threads[HELPER_READ_PREFETCH_WORKERS];
	pthread_t read_workers[HELPER_READ_WORKERS];
	struct read_worker_arg read_worker_args[HELPER_READ_WORKERS];
	pthread_mutex_t read_prefetch_mu;
	pthread_cond_t read_prefetch_cv;
	bool read_prefetch_thread_running;
	bool read_prefetch_thread_stop;
	uint32_t read_foreground_inflight;
	uint64_t read_meta_cache_clock;
	struct read_meta_cache_entry read_meta_cache[HELPER_READ_META_CACHE_MAX];
	uint64_t read_affinity_clock;
	struct read_affinity_entry read_affinity_cache[HELPER_READ_AFFINITY_CACHE_MAX];
	uint64_t read_prefetch_data_clock;
	struct read_prefetch_request read_prefetch_queue[HELPER_READ_PREFETCH_QUEUE_MAX];
	struct read_prefetch_data_entry read_prefetch_data[HELPER_READ_PREFETCH_DATA_CACHE_MAX];
	pthread_mutex_t read_job_mu;
	pthread_cond_t read_job_cv;
	struct read_job read_jobs[HELPER_READ_JOB_QUEUE_MAX];
	uint64_t read_stream_affinity_clock;
	uint32_t read_stream_next_worker;
	struct read_stream_affinity_entry read_stream_affinity[HELPER_READ_STREAM_AFFINITY_MAX];
	bool read_worker_running;
	bool read_worker_stop;
	pthread_mutex_t getattr_cache_mu;
	pthread_cond_t getattr_cache_cv;
	uint64_t getattr_cache_clock;
	struct getattr_cache_entry getattr_cache[HELPER_GETATTR_CACHE_MAX];
	int ctrl_fd;
	pthread_mutex_t ctrl_write_mu;
	bool write_meta_valid;
	uint32_t write_inode;
	uint32_t write_chunk_idx;
	struct chunk_meta write_meta;
	uint64_t write_file_size;
	uint32_t write_min_chunk_off;
	uint32_t write_max_chunk_end;
	int write_cs_fd;
	uint32_t write_cs_ip;
	uint16_t write_cs_port;
	bool write_cs_active;
	uint32_t write_next_writeid;
};

static int session_write_chunk_end(struct helper_session *s,
				   uint32_t inode, uint32_t chunk_idx,
				   uint64_t chunkid, uint64_t file_size,
				   uint32_t chunk_off, uint32_t write_size);
static int session_finalize_active_write(struct helper_session *s);
static int session_read_chunk_meta(struct helper_session *s,
				   uint32_t inode, uint32_t chunk_idx,
				   struct chunk_meta *m);
static const struct cs_loc *choose_read_replica(struct helper_session *s,
						const struct read_state *rs,
						uint32_t inode,
						uint32_t chunk_idx,
						const struct chunk_meta *m);
static int cs_read_data(const struct chunk_meta *m, uint32_t chunk_off,
			uint32_t want, uint8_t *dst, uint32_t *got);
static int cs_send_packet(int fd, uint32_t type, const void *data, uint32_t len);
static void read_state_init(struct read_state *rs);
static void read_state_drop_cs(struct read_state *rs);
static void read_state_reset_scoreboard(struct read_state *rs);
static void read_state_reset_prefetch(struct read_state *rs);
static void read_state_destroy(struct read_state *rs);
static int read_state_prepare_window(struct read_state *rs,
				     uint32_t start, uint32_t len,
				     uint8_t **dst_out);
static void read_state_commit_window(struct read_state *rs,
				     uint32_t start, uint32_t len);
static int read_state_read_data(struct helper_session *s,
				struct read_state *rs,
				const struct mfs_ctrl_read_req *req,
				uint8_t **out, uint32_t *out_len);
static uint32_t session_select_read_worker(struct helper_session *s,
					   const struct mfs_ctrl_read_req *req);
static inline uint64_t mfs_read_band(loff_t offset);
static inline uint64_t mfs_read_chunk_band(uint32_t chunk_idx);
static inline uint32_t mfs_read_chunks_per_band(void);
static int read_state_fetch_window(struct helper_session *s,
				   struct read_state *rs,
				   uint32_t inode,
				   uint32_t chunk_idx,
				   const struct chunk_meta *m,
				   uint32_t chunk_off,
				   uint32_t fetch_len,
				   uint8_t *fetch_buf,
				   uint32_t *got_out);
static int connect_tcp(const char *host, uint16_t port, int timeout_ms);
static int helper_read_timeout_ms(struct helper_session *s);
static int helper_read_io_timeout_ms(struct helper_session *s);

static uint64_t monotonic_ms(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

static int helper_read_timeout_ms(struct helper_session *s)
{
	uint32_t active_reads = 0;

	pthread_mutex_lock(&s->read_prefetch_mu);
	active_reads = s->read_foreground_inflight;
	pthread_mutex_unlock(&s->read_prefetch_mu);

	if (active_reads >= 4)
		return HELPER_READ_TIMEOUT_FANOUT_MS;
	if (active_reads >= 2)
		return HELPER_READ_TIMEOUT_SHARED_MS;
	if (active_reads >= 1)
		return HELPER_READ_TIMEOUT_MULTI_MS;
	return HELPER_READ_TIMEOUT_SINGLE_MS;
}

static int helper_read_io_timeout_ms(struct helper_session *s)
{
	uint32_t active_reads = 0;

	pthread_mutex_lock(&s->read_prefetch_mu);
	active_reads = s->read_foreground_inflight;
	pthread_mutex_unlock(&s->read_prefetch_mu);

	if (active_reads >= 4)
		return HELPER_READ_IO_TIMEOUT_FANOUT_MS;
	if (active_reads >= 1)
		return HELPER_READ_IO_TIMEOUT_MULTI_MS;
	return HELPER_READ_IO_TIMEOUT_SINGLE_MS;
}

static inline uint64_t mfs_read_band(loff_t offset)
{
	return ((uint64_t)offset) >> HELPER_READ_STREAM_BAND_SHIFT;
}

static inline uint64_t mfs_read_chunk_band(uint32_t chunk_idx)
{
	return mfs_read_band((loff_t)chunk_idx * (loff_t)MFSCHUNKSIZE);
}

static inline uint32_t mfs_read_chunks_per_band(void)
{
	uint64_t band_bytes = 1ULL << HELPER_READ_STREAM_BAND_SHIFT;
	uint32_t chunks = (uint32_t)(band_bytes / MFSCHUNKSIZE);

	if (chunks == 0)
		chunks = 1;
	if (chunks > HELPER_READ_STRIPE_META_MAX)
		chunks = HELPER_READ_STRIPE_META_MAX;
	return chunks;
}

static uint32_t session_select_read_worker(struct helper_session *s,
					   const struct mfs_ctrl_read_req *req)
{
	uint64_t band;
	struct read_stream_affinity_entry *victim = NULL;
	size_t i;

	if (!req)
		return 0;

	band = mfs_read_band(req->offset);
	for (i = 0; i < HELPER_READ_STREAM_AFFINITY_MAX; i++) {
		struct read_stream_affinity_entry *ent = &s->read_stream_affinity[i];

		if (!ent->valid)
			continue;
		if (ent->inode != req->inode || ent->band != band)
			continue;
		ent->stamp = ++s->read_stream_affinity_clock;
		return ent->worker_id % HELPER_READ_WORKERS;
	}

	for (i = 0; i < HELPER_READ_STREAM_AFFINITY_MAX; i++) {
		struct read_stream_affinity_entry *ent = &s->read_stream_affinity[i];

		if (!victim || !victim->valid ||
		    (ent->valid && ent->stamp < victim->stamp) ||
		    !ent->valid) {
			victim = ent;
			if (!ent->valid)
				break;
		}
	}

	if (!victim)
		return 0;

	memset(victim, 0, sizeof(*victim));
	victim->valid = true;
	victim->inode = req->inode;
	victim->band = band;
	victim->worker_id = s->read_stream_next_worker++ % HELPER_READ_WORKERS;
	victim->stamp = ++s->read_stream_affinity_clock;
	return victim->worker_id;
}

static void session_reset_read_meta_cache(struct helper_session *s)
{
	size_t i;

	pthread_mutex_lock(&s->read_prefetch_mu);
	memset(s->read_meta_cache, 0, sizeof(s->read_meta_cache));
	s->read_meta_cache_clock = 0;
	memset(s->read_affinity_cache, 0, sizeof(s->read_affinity_cache));
	s->read_affinity_clock = 0;
	s->read_foreground_inflight = 0;
	memset(s->read_prefetch_queue, 0, sizeof(s->read_prefetch_queue));
	for (i = 0; i < HELPER_READ_PREFETCH_DATA_CACHE_MAX; i++) {
		free(s->read_prefetch_data[i].buf);
		memset(&s->read_prefetch_data[i], 0, sizeof(s->read_prefetch_data[i]));
	}
	s->read_prefetch_data_clock = 0;
	pthread_cond_broadcast(&s->read_prefetch_cv);
	pthread_mutex_unlock(&s->read_prefetch_mu);
}

static void read_state_init(struct read_state *rs)
{
	memset(rs, 0, sizeof(*rs));
	rs->cs_fd = -1;
	rs->prefetch_len = HELPER_READ_PREFETCH_BASE;
}

static void read_state_reset_scoreboard(struct read_state *rs)
{
	free(rs->cache_buf);
	rs->cache_buf = NULL;
	rs->cache_cap = 0;
	rs->cache_base = 0;
	rs->scoreboard_count = 0;
	memset(rs->scoreboard, 0, sizeof(rs->scoreboard));
}

static void read_state_drop_cs(struct read_state *rs)
{
	if (rs->cs_fd >= 0) {
		close(rs->cs_fd);
		rs->cs_fd = -1;
	}
	rs->cs_ip = 0;
	rs->cs_port = 0;
}

static void read_state_reset_prefetch(struct read_state *rs)
{
	rs->prefetch_len = HELPER_READ_PREFETCH_BASE;
	rs->last_end = 0;
	rs->seq_bytes = 0;
}

static void read_state_reset_stripe(struct read_state *rs)
{
	rs->stripe_band = 0;
	rs->stripe_base_chunk = 0;
	rs->stripe_chunk_count = 0;
	memset(rs->stripe_meta, 0, sizeof(rs->stripe_meta));
}

static void read_state_destroy(struct read_state *rs)
{
	read_state_drop_cs(rs);
	read_state_reset_scoreboard(rs);
	read_state_reset_prefetch(rs);
	read_state_reset_stripe(rs);
	rs->meta_valid = false;
	rs->meta_status = 0;
	rs->inode = 0;
	rs->chunk_idx = 0;
	memset(&rs->meta, 0, sizeof(rs->meta));
	rs->preferred_ip = 0;
	rs->preferred_port = 0;
}

static bool cs_loc_matches(const struct cs_loc *a, const struct cs_loc *b)
{
	if (!a || !b)
		return false;
	return a->ip == b->ip && a->port == b->port;
}

static void session_reset_read_scoreboard(struct helper_session *s)
{
	free(s->read_cache_buf);
	s->read_cache_buf = NULL;
	s->read_cache_cap = 0;
	s->read_cache_base = 0;
	s->read_scoreboard_count = 0;
	memset(s->read_scoreboard, 0, sizeof(s->read_scoreboard));
}

static void session_reset_read_prefetch(struct helper_session *s)
{
	s->read_prefetch_len = HELPER_READ_PREFETCH_BASE;
	s->read_last_end = 0;
	s->read_seq_bytes = 0;
}

static int session_prepare_read_window(struct helper_session *s,
				       uint32_t start, uint32_t len,
				       uint8_t **dst_out)
{
	struct read_state rs = {
		.cache_buf = s->read_cache_buf,
		.cache_cap = s->read_cache_cap,
		.cache_base = s->read_cache_base,
		.scoreboard_count = s->read_scoreboard_count,
		.prefetch_len = s->read_prefetch_len,
	};
	int ret;

	memcpy(rs.scoreboard, s->read_scoreboard, sizeof(rs.scoreboard));
	ret = read_state_prepare_window(&rs, start, len, dst_out);
	s->read_cache_buf = rs.cache_buf;
	s->read_cache_cap = rs.cache_cap;
	s->read_cache_base = rs.cache_base;
	s->read_scoreboard_count = rs.scoreboard_count;
	memcpy(s->read_scoreboard, rs.scoreboard, sizeof(s->read_scoreboard));
	return ret;
}

static void session_commit_read_window(struct helper_session *s,
				       uint32_t start, uint32_t len)
{
	struct read_state rs = {
		.cache_buf = s->read_cache_buf,
		.cache_cap = s->read_cache_cap,
		.cache_base = s->read_cache_base,
		.scoreboard_count = s->read_scoreboard_count,
	};

	memcpy(rs.scoreboard, s->read_scoreboard, sizeof(rs.scoreboard));
	read_state_commit_window(&rs, start, len);
	s->read_cache_base = rs.cache_base;
	s->read_scoreboard_count = rs.scoreboard_count;
	memcpy(s->read_scoreboard, rs.scoreboard, sizeof(s->read_scoreboard));
}

static bool session_lookup_read_meta_cache(struct helper_session *s,
					   uint32_t inode, uint32_t chunk_idx,
					   struct chunk_meta *m,
					   int *status)
{
	bool found = false;
	size_t i;

	pthread_mutex_lock(&s->read_prefetch_mu);
	for (i = 0; i < HELPER_READ_META_CACHE_MAX; i++) {
		struct read_meta_cache_entry *ent = &s->read_meta_cache[i];

		if (!ent->valid)
			continue;
		if (ent->inode != inode || ent->chunk_idx != chunk_idx)
			continue;
		if (ent->pending)
			continue;
		ent->stamp = ++s->read_meta_cache_clock;
		if (status)
			*status = ent->status;
		if (m)
			*m = ent->meta;
		found = true;
		break;
	}
	pthread_mutex_unlock(&s->read_prefetch_mu);
	return found;
}

static bool session_acquire_read_meta_fetch(struct helper_session *s,
					    uint32_t inode, uint32_t chunk_idx)
{
	struct read_meta_cache_entry *victim = NULL;
	size_t i;

	pthread_mutex_lock(&s->read_prefetch_mu);
	for (;;) {
		victim = NULL;
		for (i = 0; i < HELPER_READ_META_CACHE_MAX; i++) {
			struct read_meta_cache_entry *ent = &s->read_meta_cache[i];

			if (ent->valid && ent->inode == inode && ent->chunk_idx == chunk_idx) {
				if (ent->pending) {
					pthread_cond_wait(&s->read_prefetch_cv, &s->read_prefetch_mu);
					victim = NULL;
					break;
				}
				pthread_mutex_unlock(&s->read_prefetch_mu);
				return false;
			}
			if (!victim || (!victim->valid && !ent->valid) ||
			    (!ent->valid && victim->valid) ||
			    (victim->valid && ent->valid && ent->stamp < victim->stamp)) {
				victim = ent;
			}
		}
		if (victim)
			break;
	}
	memset(victim, 0, sizeof(*victim));
	victim->valid = true;
	victim->pending = true;
	victim->inode = inode;
	victim->chunk_idx = chunk_idx;
	victim->stamp = ++s->read_meta_cache_clock;
	pthread_mutex_unlock(&s->read_prefetch_mu);
	return true;
}

static void session_abort_read_meta_fetch(struct helper_session *s,
					  uint32_t inode, uint32_t chunk_idx)
{
	size_t i;

	pthread_mutex_lock(&s->read_prefetch_mu);
	for (i = 0; i < HELPER_READ_META_CACHE_MAX; i++) {
		struct read_meta_cache_entry *ent = &s->read_meta_cache[i];

		if (!ent->valid || !ent->pending)
			continue;
		if (ent->inode != inode || ent->chunk_idx != chunk_idx)
			continue;
		memset(ent, 0, sizeof(*ent));
		break;
	}
	pthread_cond_broadcast(&s->read_prefetch_cv);
	pthread_mutex_unlock(&s->read_prefetch_mu);
}

static bool session_lookup_read_affinity(struct helper_session *s,
					 uint32_t inode,
					 uint32_t *ip_out,
					 uint16_t *port_out)
{
	bool found = false;
	size_t i;

	pthread_mutex_lock(&s->read_prefetch_mu);
	for (i = 0; i < HELPER_READ_AFFINITY_CACHE_MAX; i++) {
		struct read_affinity_entry *ent = &s->read_affinity_cache[i];

		if (!ent->valid || ent->inode != inode)
			continue;
		ent->stamp = ++s->read_affinity_clock;
		if (ip_out)
			*ip_out = ent->preferred_ip;
		if (port_out)
			*port_out = ent->preferred_port;
		found = true;
		break;
	}
	pthread_mutex_unlock(&s->read_prefetch_mu);
	return found;
}

static void session_store_read_affinity(struct helper_session *s,
					uint32_t inode,
					uint32_t preferred_ip,
					uint16_t preferred_port)
{
	struct read_affinity_entry *victim = NULL;
	size_t i;

	if (!preferred_ip || !preferred_port)
		return;

	pthread_mutex_lock(&s->read_prefetch_mu);
	for (i = 0; i < HELPER_READ_AFFINITY_CACHE_MAX; i++) {
		struct read_affinity_entry *ent = &s->read_affinity_cache[i];

		if (ent->valid && ent->inode == inode) {
			victim = ent;
			break;
		}
		if (!victim || !victim->valid || ent->stamp < victim->stamp)
			victim = ent;
	}
	if (victim) {
		memset(victim, 0, sizeof(*victim));
		victim->valid = true;
		victim->inode = inode;
		victim->preferred_ip = preferred_ip;
		victim->preferred_port = preferred_port;
		victim->stamp = ++s->read_affinity_clock;
	}
	pthread_mutex_unlock(&s->read_prefetch_mu);
}

static void session_store_read_meta_cache(struct helper_session *s,
					  uint32_t inode, uint32_t chunk_idx,
					  int status,
					  const struct chunk_meta *m)
{
	struct read_meta_cache_entry *victim = NULL;
	size_t i;

	pthread_mutex_lock(&s->read_prefetch_mu);
	for (i = 0; i < HELPER_READ_META_CACHE_MAX; i++) {
		struct read_meta_cache_entry *ent = &s->read_meta_cache[i];

		if (ent->valid &&
		    ent->inode == inode &&
		    ent->chunk_idx == chunk_idx) {
			victim = ent;
			break;
		}
		if (!victim || !victim->valid || ent->stamp < victim->stamp)
			victim = ent;
	}
	if (victim) {
		memset(victim, 0, sizeof(*victim));
		victim->valid = true;
		victim->pending = false;
		victim->inode = inode;
		victim->chunk_idx = chunk_idx;
		victim->status = status;
		if (m)
			victim->meta = *m;
		victim->stamp = ++s->read_meta_cache_clock;
	}
	pthread_cond_broadcast(&s->read_prefetch_cv);
	pthread_mutex_unlock(&s->read_prefetch_mu);
}

static void session_schedule_read_meta_prefetch(struct helper_session *s,
						uint32_t inode,
						uint32_t chunk_idx,
						uint32_t chunk_off,
						uint32_t fetch_len)
{
	size_t i;
	size_t free_slot = HELPER_READ_PREFETCH_QUEUE_MAX;

	pthread_mutex_lock(&s->read_prefetch_mu);
	if (!s->read_prefetch_thread_running || s->read_prefetch_thread_stop) {
		pthread_mutex_unlock(&s->read_prefetch_mu);
		return;
	}
	for (i = 0; i < HELPER_READ_PREFETCH_DATA_CACHE_MAX; i++) {
		struct read_prefetch_data_entry *ent = &s->read_prefetch_data[i];

		if (ent->valid &&
		    ent->inode == inode &&
		    ent->chunk_idx == chunk_idx &&
		    ent->base == chunk_off &&
		    ent->len >= fetch_len) {
			pthread_mutex_unlock(&s->read_prefetch_mu);
			return;
		}
	}
	for (i = 0; i < HELPER_READ_PREFETCH_QUEUE_MAX; i++) {
		struct read_prefetch_request *req = &s->read_prefetch_queue[i];

		if (req->valid &&
		    req->inode == inode &&
		    req->chunk_idx == chunk_idx &&
		    req->chunk_off == chunk_off &&
		    req->fetch_len == fetch_len) {
			pthread_mutex_unlock(&s->read_prefetch_mu);
			return;
		}
		if (!req->valid && free_slot == HELPER_READ_PREFETCH_QUEUE_MAX)
			free_slot = i;
	}
	if (free_slot == HELPER_READ_PREFETCH_QUEUE_MAX) {
		pthread_mutex_unlock(&s->read_prefetch_mu);
		return;
	}
	s->read_prefetch_queue[free_slot].valid = true;
	s->read_prefetch_queue[free_slot].inode = inode;
	s->read_prefetch_queue[free_slot].chunk_idx = chunk_idx;
	s->read_prefetch_queue[free_slot].chunk_off = chunk_off;
	s->read_prefetch_queue[free_slot].fetch_len = fetch_len;
	pthread_cond_broadcast(&s->read_prefetch_cv);
	pthread_mutex_unlock(&s->read_prefetch_mu);
}

static bool session_try_use_prefetched_read(struct helper_session *s,
					    uint32_t inode, uint32_t chunk_idx,
					    uint32_t chunk_off, uint32_t want)
{
	(void)s;
	(void)inode;
	(void)chunk_idx;
	(void)chunk_off;
	(void)want;
	return false;
#if 0
	struct read_prefetch_data_entry *best = NULL;
	uint8_t *tmp = NULL;
	uint32_t base = 0;
	uint32_t len = 0;
	uint8_t *dst = NULL;
	size_t i;
	int ret;

	pthread_mutex_lock(&s->read_prefetch_mu);
	for (i = 0; i < HELPER_READ_PREFETCH_DATA_CACHE_MAX; i++) {
		struct read_prefetch_data_entry *ent = &s->read_prefetch_data[i];

		if (!ent->valid)
			continue;
		if (ent->inode != inode || ent->chunk_idx != chunk_idx)
			continue;
		if (ent->base > chunk_off || ent->base + ent->len < chunk_off + want)
			continue;
		if (!best || ent->stamp > best->stamp)
			best = ent;
	}
	if (best) {
		base = best->base;
		len = best->len;
		best->stamp = ++s->read_prefetch_data_clock;
		tmp = malloc(len);
		if (tmp)
			memcpy(tmp, best->buf, len);
	}
	pthread_mutex_unlock(&s->read_prefetch_mu);

	if (!tmp)
		return false;

	ret = session_prepare_read_window(s, base, len, &dst);
	if (ret) {
		free(tmp);
		return false;
	}
	memcpy(dst, tmp, len);
	free(tmp);
	session_commit_read_window(s, base, len);
	return true;
#endif
}

static void session_schedule_read_ahead(struct helper_session *s,
					uint32_t inode,
					uint32_t chunk_idx)
{
	(void)s;
	(void)inode;
	(void)chunk_idx;
}

static const struct cs_loc *choose_read_replica(struct helper_session *s,
						const struct read_state *rs,
						uint32_t inode,
						uint32_t chunk_idx,
						const struct chunk_meta *m)
{
	struct chunk_meta future_meta;
	int future_status = 0;
	uint32_t session_preferred_ip = 0;
	uint16_t session_preferred_port = 0;
	uint32_t best_score = 0;
	const struct cs_loc *best = NULL;
	uint32_t i;
	uint32_t j;
	uint32_t k;

	if (!m || m->loc_count == 0)
		return NULL;

	(void)session_lookup_read_affinity(s, inode,
					       &session_preferred_ip,
					       &session_preferred_port);

	for (i = 0; i < m->loc_count; i++) {
		if (rs->cs_fd >= 0 &&
		    rs->cs_ip == m->locs[i].ip &&
		    rs->cs_port == m->locs[i].port)
			return &m->locs[i];
	}

	for (i = 0; i < m->loc_count; i++) {
		uint32_t score = 0;

		if (rs->preferred_ip == m->locs[i].ip &&
		    rs->preferred_port == m->locs[i].port)
			score += 8;
		if (session_preferred_ip == m->locs[i].ip &&
		    session_preferred_port == m->locs[i].port)
			score += 16;

		for (k = 1; k <= HELPER_READ_REPLICA_LOOKAHEAD; k++) {
			if (!session_lookup_read_meta_cache(s, inode, chunk_idx + k,
							    &future_meta, &future_status) ||
			    future_status != 0)
				continue;
			for (j = 0; j < future_meta.loc_count; j++) {
				if (m->locs[i].ip == future_meta.locs[j].ip &&
				    m->locs[i].port == future_meta.locs[j].port) {
					score += (HELPER_READ_REPLICA_LOOKAHEAD + 1U - k);
					break;
				}
			}
		}
		if (!best || score > best_score) {
			best = &m->locs[i];
			best_score = score;
		}
	}

	if (best)
		return best;

	for (i = 0; i < m->loc_count; i++) {
		if (rs->preferred_ip == m->locs[i].ip &&
		    rs->preferred_port == m->locs[i].port)
			return &m->locs[i];
	}

	return &m->locs[0];
}

static void read_state_note_progress(struct read_state *rs,
				     uint32_t inode,
				     uint64_t start,
				     uint32_t len)
{
	uint64_t end = start + len;

	if (len == 0)
		return;
	if (rs->inode != inode) {
		rs->seq_bytes = len;
		rs->prefetch_len = HELPER_READ_PREFETCH_BASE;
	} else if (start == rs->last_end) {
		rs->seq_bytes += len;
		if (rs->seq_bytes >= HELPER_READ_SEQ_TRIGGER &&
		    rs->prefetch_len < HELPER_READ_PREFETCH_MAX) {
			rs->prefetch_len <<= 1;
			if (rs->prefetch_len > HELPER_READ_PREFETCH_MAX)
				rs->prefetch_len = HELPER_READ_PREFETCH_MAX;
		}
	} else {
		rs->seq_bytes = len;
		rs->prefetch_len = HELPER_READ_PREFETCH_BASE;
	}
	rs->last_end = end;
}

static void read_state_add_coverage(struct read_state *rs,
				    uint32_t start, uint32_t end)
{
	if (start >= end)
		return;

	/*
	 * Keep the first implementation deliberately simple: one proven span
	 * representing the cached window we most recently fetched. The fixed
	 * scoreboard shape stays in place so we can later expand to multiple
	 * spans or true OOO retirement without changing the session model.
	 */
	rs->scoreboard[0].start = start;
	rs->scoreboard[0].end = end;
	rs->scoreboard_count = 1;
}

static uint32_t read_state_cached_prefix_len(struct read_state *rs,
					     uint32_t start, uint32_t want)
{
	uint32_t end = start + want;
	struct range_span span;

	if (!rs->cache_buf || want == 0)
		return 0;
	if (rs->scoreboard_count == 0)
		return 0;
	if (start < rs->cache_base)
		return 0;
	if (end > rs->cache_base + rs->cache_cap)
		end = rs->cache_base + rs->cache_cap;
	if (start >= end)
		return 0;
	span = rs->scoreboard[0];
	if (span.start > start)
		return 0;
	if (span.end <= start)
		return 0;
	if (span.end < end)
		return span.end - start;
	return end - start;
}

static uint32_t read_state_copy_from_cache(struct read_state *rs,
					   uint32_t start, uint32_t want,
					   uint8_t *dst)
{
	uint32_t covered = read_state_cached_prefix_len(rs, start, want);

	if (!covered)
		return 0;
	memcpy(dst,
	       rs->cache_buf + (start - rs->cache_base),
	       covered);
	return covered;
}

static int read_state_prepare_window(struct read_state *rs,
				     uint32_t start, uint32_t len,
				     uint8_t **dst_out)
{
	if (!dst_out)
		return -EINVAL;
	if (len == 0)
		return 0;

	if (len > HELPER_READ_CACHE_MAX)
		len = HELPER_READ_CACHE_MAX;

	if (!rs->cache_buf || rs->cache_cap < len) {
		uint8_t *tmp = realloc(rs->cache_buf, len);
		if (!tmp)
			return -ENOMEM;
		rs->cache_buf = tmp;
		rs->cache_cap = len;
	}

	rs->cache_base = start;
	rs->scoreboard_count = 0;
	memset(rs->scoreboard, 0, sizeof(rs->scoreboard));
	*dst_out = rs->cache_buf;
	return 0;
}

static void read_state_commit_window(struct read_state *rs,
				     uint32_t start, uint32_t len)
{
	rs->cache_base = start;
	rs->scoreboard_count = 0;
	memset(rs->scoreboard, 0, sizeof(rs->scoreboard));
	read_state_add_coverage(rs, start, start + len);
}

static volatile sig_atomic_t g_stop;
static int g_verbose;

static void handle_sigint(int signo)
{
	(void)signo;
	g_stop = 1;
}

static void vlog(const char *fmt, ...)
{
	va_list ap;

	if (!g_verbose)
		return;
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	fputc('\n', stderr);
	va_end(ap);
}

static uint16_t get16be(const uint8_t *p)
{
	return ((uint16_t)p[0] << 8) | p[1];
}

static uint32_t get32be(const uint8_t *p)
{
	return ((uint32_t)p[0] << 24) |
	       ((uint32_t)p[1] << 16) |
	       ((uint32_t)p[2] << 8) |
	       (uint32_t)p[3];
}

static uint64_t get64be(const uint8_t *p)
{
	uint64_t hi = get32be(p);
	uint64_t lo = get32be(p + 4);
	return (hi << 32) | lo;
}

static void put16be(uint8_t *p, uint16_t v)
{
	p[0] = (v >> 8) & 0xff;
	p[1] = v & 0xff;
}

static void put32be(uint8_t *p, uint32_t v)
{
	p[0] = (v >> 24) & 0xff;
	p[1] = (v >> 16) & 0xff;
	p[2] = (v >> 8) & 0xff;
	p[3] = v & 0xff;
}

static void put64be(uint8_t *p, uint64_t v)
{
	put32be(p, (uint32_t)(v >> 32));
	put32be(p + 4, (uint32_t)v);
}

static uint8_t disp_type_to_wire(uint8_t t)
{
	switch (t) {
	case DISP_TYPE_FILE:
	case TYPE_FILE:
		return MFS_WIRE_TYPE_FILE;
	case DISP_TYPE_DIRECTORY:
	case TYPE_DIRECTORY:
		return MFS_WIRE_TYPE_DIR;
	case DISP_TYPE_SYMLINK:
	case TYPE_SYMLINK:
		return MFS_WIRE_TYPE_SYMLINK;
	case DISP_TYPE_FIFO:
	case TYPE_FIFO:
		return MFS_WIRE_TYPE_FIFO;
	case DISP_TYPE_BLOCKDEV:
	case TYPE_BLOCKDEV:
		return MFS_WIRE_TYPE_BLOCK;
	case DISP_TYPE_CHARDEV:
	case TYPE_CHARDEV:
		return MFS_WIRE_TYPE_CHAR;
	case DISP_TYPE_SOCKET:
	case TYPE_SOCKET:
		return MFS_WIRE_TYPE_SOCK;
	default:
		return MFS_WIRE_TYPE_UNKNOWN;
	}
}

static int parse_attr_record(const uint8_t *data, size_t len,
			     struct mfs_wire_attr *out,
			     size_t *consumed)
{
	uint8_t flags;
	uint16_t mode;
	uint8_t type;
	size_t attr_size;
	const uint8_t *p = data;

	if (len < 35)
		return -EPROTO;

	if (data[0] < 64) {
		flags = p[0];
		mode = get16be(p + 1);
		type = (mode >> 12) & 0x0f;
		mode &= 0x0fff;
		p += 3;
	} else {
		type = disp_type_to_wire(p[0] & 0x7f);
		mode = get16be(p + 1) & 0x0fff;
		flags = (get16be(p + 1) >> 12) & 0x0f;
		p += 3;
	}

	out->type = type;
	out->mattr = flags;
	out->mode = mode;
	out->uid = get32be(p);
	p += 4;
	out->gid = get32be(p);
	p += 4;
	out->atime = get32be(p);
	p += 4;
	out->mtime = get32be(p);
	p += 4;
	out->ctime = get32be(p);
	p += 4;
	out->nlink = get32be(p);
	p += 4;
	out->rdev = 0;
	out->size = 0;
	out->winattr = 0;

	if (type == MFS_WIRE_TYPE_BLOCK || type == MFS_WIRE_TYPE_CHAR) {
		uint16_t major, minor;
		if ((size_t)(p - data) + 8 > len)
			return -EPROTO;
		major = get16be(p);
		minor = get16be(p + 2);
		out->rdev = ((uint32_t)major << 16) | minor;
		p += 8;
	} else {
		if ((size_t)(p - data) + 8 > len)
			return -EPROTO;
		out->size = get64be(p);
		p += 8;
	}

	attr_size = (size_t)(p - data);
	if (len >= attr_size + 1) {
		out->winattr = p[0];
		attr_size++;
	}
	if (consumed)
		*consumed = attr_size;
	return 0;
}

static int connect_tcp(const char *host, uint16_t port, int timeout_ms)
{
	char portbuf[16];
	struct addrinfo hints;
	struct addrinfo *ai = NULL;
	struct addrinfo *it;
	int ret;
	int fd = -1;

	snprintf(portbuf, sizeof(portbuf), "%u", (unsigned)port);
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	ret = getaddrinfo(host, portbuf, &hints, &ai);
	if (ret != 0)
		return -EHOSTUNREACH;

	for (it = ai; it; it = it->ai_next) {
		struct pollfd pfd;
		int flags;
		int soerr = 0;
		socklen_t soerr_len = sizeof(soerr);

		fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
		if (fd < 0)
			continue;

		flags = fcntl(fd, F_GETFL, 0);
		if (flags >= 0)
			(void)fcntl(fd, F_SETFL, flags | O_NONBLOCK);

		ret = connect(fd, it->ai_addr, it->ai_addrlen);
		if (ret == 0)
			goto connected;
		if (ret < 0 && errno != EINPROGRESS) {
			close(fd);
			fd = -1;
			continue;
		}

		pfd.fd = fd;
		pfd.events = POLLOUT;
		pfd.revents = 0;
		ret = poll(&pfd, 1, timeout_ms);
		if (ret <= 0 || !(pfd.revents & (POLLOUT | POLLERR | POLLHUP))) {
			close(fd);
			fd = -1;
			continue;
		}

		if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &soerr_len) < 0 || soerr != 0) {
			close(fd);
			fd = -1;
			continue;
		}

connected:
		if (flags >= 0)
			(void)fcntl(fd, F_SETFL, flags);
		else
			(void)fcntl(fd, F_SETFL, 0);
		break;
	}

	freeaddrinfo(ai);
	if (fd < 0)
		return -ECONNREFUSED;
	return fd;
}

static void set_socket_timeouts(int fd, int timeout_ms)
{
	struct timeval tv;

	if (fd < 0 || timeout_ms <= 0)
		return;

	tv.tv_sec = timeout_ms / 1000;
	tv.tv_usec = (timeout_ms % 1000) * 1000;
	setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

static int read_full(int fd, void *buf, size_t len)
{
	uint8_t *p = buf;

	while (len) {
		ssize_t n = read(fd, p, len);
		if (n == 0)
			return -ECONNRESET;
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return -errno;
		}
		p += n;
		len -= (size_t)n;
	}
	return 0;
}

static int write_full(int fd, const void *buf, size_t len)
{
	const uint8_t *p = buf;

	while (len) {
		ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
		if (n < 0 && (errno == ENOTSOCK || errno == EOPNOTSUPP || errno == EINVAL))
			n = write(fd, p, len);
		if (n == 0)
			return -EPIPE;
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return -errno;
		}
		p += n;
		len -= (size_t)n;
	}
	return 0;
}

static int master_send_packet(int fd, uint32_t type, const void *data, uint32_t len)
{
	uint8_t hdr[8];
	int ret;

	if (len && !data)
		return -EINVAL;

	put32be(hdr, type);
	put32be(hdr + 4, len);
	ret = write_full(fd, hdr, sizeof(hdr));
	if (ret)
		return ret;
	if (len)
		ret = write_full(fd, data, len);
	return ret;
}

static int master_recv_packet(int fd, uint32_t *type, uint8_t **data, uint32_t *len)
{
	uint8_t hdr[8];
	int ret;

	if (!type || !data || !len)
		return -EINVAL;
	*type = 0;
	*len = 0;
	*data = NULL;

	ret = read_full(fd, hdr, sizeof(hdr));
	if (ret)
		return ret;

	*type = get32be(hdr);
	*len = get32be(hdr + 4);
	if (*len > 32 * 1024 * 1024)
		return -EMSGSIZE;

	if (*len) {
		*data = malloc(*len);
		if (!*data)
			return -ENOMEM;
		ret = read_full(fd, *data, *len);
		if (ret) {
			free(*data);
			*data = NULL;
			return ret;
		}
	} else {
		*data = NULL;
	}
	return 0;
}

static bool req_has_msgid(uint32_t req_type, uint32_t req_len)
{
	if (req_len < 4)
		return false;
	if (req_type == CLTOMA_FUSE_REGISTER)
		return false;
	return true;
}

static bool packet_is_unsolicited(uint32_t type, const uint8_t *data, uint32_t len)
{
	/* Master keep-alive packet: ANTOAN_NOP + msgid(0) */
	if (type == ANTOAN_NOP && len == 4)
		return true;
	/* Unsolicited notifications/events from master are sent with msgid 0. */
	if (len >= 4 && get32be(data) == 0)
		return true;
	return false;
}

static bool packet_is_benign_keepalive(uint32_t type, const uint8_t *data, uint32_t len)
{
	if (type == ANTOAN_NOP && len == 4)
		return true;
	if (type == 0 && len == 4 && data && get32be(data) == 0)
		return true;
	return false;
}

static bool master_conn_broken(int ret)
{
	return ret == -EPIPE || ret == -ECONNRESET || ret == -ENOTCONN;
}

static void session_reset_state(struct helper_session *s)
{
	if (s->master_fd >= 0) {
		close(s->master_fd);
		s->master_fd = -1;
	}
	s->active = false;
	s->next_msgid = 1;
	s->session_id = 0;
	s->master_version = 0;
	s->attr_size = 36;
	s->master_host[0] = '\0';
	s->master_port = 0;
	s->subdir[0] = '\0';
	s->master_last_nop = time(NULL);
	if (s->read_cs_fd >= 0) {
		close(s->read_cs_fd);
		s->read_cs_fd = -1;
	}
	s->read_meta_valid = false;
	s->read_meta_status = 0;
	s->read_inode = 0;
	s->read_chunk_idx = 0;
	memset(&s->read_meta, 0, sizeof(s->read_meta));
	s->read_cs_ip = 0;
	s->read_cs_port = 0;
	session_reset_read_scoreboard(s);
	session_reset_read_prefetch(s);
	session_reset_read_meta_cache(s);
	if (s->write_cs_fd >= 0) {
		close(s->write_cs_fd);
		s->write_cs_fd = -1;
	}
	s->write_cs_active = false;
	s->write_next_writeid = 1;
	s->write_meta_valid = false;
	s->write_inode = 0;
	s->write_chunk_idx = 0;
	memset(&s->write_meta, 0, sizeof(s->write_meta));
	s->write_file_size = 0;
	s->write_min_chunk_off = 0;
	s->write_max_chunk_end = 0;
	s->write_cs_ip = 0;
	s->write_cs_port = 0;
}

static void session_clear_master_slot(struct helper_session *s)
{
	free(s->master_req_data);
	s->master_req_data = NULL;
	s->master_req_len = 0;
	free(s->master_rsp_data);
	s->master_rsp_data = NULL;
	s->master_rsp_len = 0;
	s->master_rsp_status = 0;
	s->master_req_pending = false;
	s->master_req_done = false;
}

static int build_register_packet(const char *subdir, uint8_t **pkt_out,
				 uint32_t *pkt_len_out)
{
	uint8_t *pkt;
	uint32_t pkt_len;
	uint32_t info_len = (uint32_t)strlen(HELPER_INFO_STR) + 1;
	uint32_t sub_len = (subdir && subdir[0]) ? (uint32_t)strlen(subdir) + 1 : 2;
	size_t off;

	if (!pkt_out || !pkt_len_out)
		return -EINVAL;

	pkt_len = 64 + 1 + 4 + 4 + info_len + 4 + sub_len;
	pkt = calloc(1, pkt_len);
	if (!pkt)
		return -ENOMEM;

	off = 0;
	memcpy(pkt + off, FUSE_REGISTER_BLOB_ACL, 64);
	off += 64;
	pkt[off++] = REGISTER_NEWSESSION;
	put16be(pkt + off, MFS_VERSMAJ);
	off += 2;
	pkt[off++] = MFS_VERSMID;
	pkt[off++] = MFS_VERSMIN;
	put32be(pkt + off, info_len);
	off += 4;
	memcpy(pkt + off, HELPER_INFO_STR, info_len);
	off += info_len;
	put32be(pkt + off, sub_len);
	off += 4;
	if (subdir && subdir[0]) {
		size_t subdir_len = strlen(subdir);
		memcpy(pkt + off, subdir, subdir_len);
		pkt[off + subdir_len] = '\0';
	} else {
		pkt[off] = '/';
	}

	*pkt_out = pkt;
	*pkt_len_out = pkt_len;
	return 0;
}

static int session_reregister(struct helper_session *s)
{
	uint8_t *pkt = NULL;
	uint8_t *rsp = NULL;
	uint32_t pkt_len = 0;
	uint32_t rsp_len = 0;
	uint32_t v1, v2;
	int ret;
	int fd;

	if (!s->master_host[0] || s->master_port == 0)
		return -EINVAL;

	fd = connect_tcp(s->master_host, s->master_port, HELPER_MASTER_TIMEOUT_MS);
	if (fd < 0)
		return fd;
	set_socket_timeouts(fd, HELPER_MASTER_TIMEOUT_MS);

	ret = build_register_packet(s->subdir, &pkt, &pkt_len);
	if (ret) {
		close(fd);
		return ret;
	}

	ret = master_send_packet(fd, CLTOMA_FUSE_REGISTER, pkt, pkt_len);
	free(pkt);
	if (ret) {
		close(fd);
		return ret;
	}
	ret = master_recv_packet(fd, &v1, &rsp, &rsp_len);
	if (ret) {
		close(fd);
		return ret;
	}
	if (v1 != MATOCL_FUSE_REGISTER) {
		free(rsp);
		close(fd);
		return -EPROTO;
	}
	if (rsp_len == 1) {
		ret = rsp[0];
		free(rsp);
		close(fd);
		return ret;
	}
	if (rsp_len < 8) {
		free(rsp);
		close(fd);
		return -EPROTO;
	}

	v1 = get32be(rsp);
	v2 = get32be(rsp + 4);
	if (v1 >= 0x00010000 && v1 <= 0x0fffffff) {
		s->master_version = v1;
		s->session_id = v2;
	} else {
		s->master_version = 0;
		s->session_id = v1;
	}
	free(rsp);

	if (s->master_fd >= 0)
		close(s->master_fd);
	s->master_fd = fd;
	s->next_msgid = 1;
	s->active = true;
	vlog("master reconnect: new session=%u master_version=0x%08x",
	     s->session_id, s->master_version);
	return 0;
}

static int master_idle_pump(struct helper_session *s)
{
	struct pollfd pfd;
	uint32_t got_type = 0;
	uint8_t *pkt = NULL;
	uint32_t pkt_len = 0;
	time_t now = time(NULL);
	int ret;
	unsigned int drained = 0;

	if (s->master_fd < 0)
		return -ENOTCONN;

	if (now - s->master_last_nop >= 2) {
		ret = master_send_packet(s->master_fd, ANTOAN_NOP, NULL, 0);
		if (ret)
			return ret;
		s->master_last_nop = now;
	}

	for (;;) {
		int timeout_ms = drained == 0 ? 250 : 0;

		pfd.fd = s->master_fd;
		pfd.events = POLLIN;
		pfd.revents = 0;
		ret = poll(&pfd, 1, timeout_ms);
		if (ret <= 0)
			return ret;
		if (!(pfd.revents & (POLLIN | POLLERR | POLLHUP)))
			return 0;
		if (pfd.revents & (POLLERR | POLLHUP))
			return -ECONNRESET;

		ret = master_recv_packet(s->master_fd, &got_type, &pkt, &pkt_len);
		if (ret)
			return ret;
		if (packet_is_unsolicited(got_type, pkt, pkt_len)) {
			bool benign_keepalive = packet_is_benign_keepalive(got_type, pkt, pkt_len);

			if (!benign_keepalive) {
				vlog("master idle: skipped unsolicited packet type=0x%08x len=%u",
				     got_type, pkt_len);
			}
			free(pkt);
			pkt = NULL;
			if (++drained >= HELPER_MASTER_MAX_SKIPPED_PKTS)
				return 0;
			continue;
		}
		vlog("master idle: unexpected packet type=0x%08x len=%u",
		     got_type, pkt_len);
		free(pkt);
		return -EPROTO;
	}
}

static int master_rpc_once(struct helper_session *s,
			   uint32_t req_type, const uint8_t *req_data, uint32_t req_len,
			   uint32_t rsp_type, uint8_t **rsp_data, uint32_t *rsp_len)
{
	uint8_t *pkt = NULL;
	uint32_t pkt_len = 0;
	uint32_t got_type;
	uint32_t req_msgid = 0;
	bool expect_msgid;
	unsigned int skipped = 0;
	int ret;

	if (s->master_fd < 0)
		return -ENOTCONN;
	if (!rsp_data || !rsp_len)
		return -EINVAL;
	*rsp_data = NULL;
	*rsp_len = 0;

	ret = master_send_packet(s->master_fd, req_type, req_data, req_len);
	if (ret) {
		vlog("master_rpc: send failed req=0x%08x rsp=0x%08x ret=%d",
		     req_type, rsp_type, ret);
		return ret;
	}

	expect_msgid = req_has_msgid(req_type, req_len);
	if (expect_msgid)
		req_msgid = get32be(req_data);

	for (;;) {
		ret = master_recv_packet(s->master_fd, &got_type, &pkt, &pkt_len);
		if (ret) {
			vlog("master_rpc: recv failed req=0x%08x rsp=0x%08x ret=%d",
			     req_type, rsp_type, ret);
			return ret;
		}
		if (got_type != rsp_type) {
			if (packet_is_unsolicited(got_type, pkt, pkt_len)) {
				bool benign_keepalive = packet_is_benign_keepalive(got_type, pkt, pkt_len);

				if (!benign_keepalive) {
					vlog("master_rpc: skipped unsolicited packet type=0x%08x len=%u",
					     got_type, pkt_len);
				}
				free(pkt);
				pkt = NULL;
				if (!benign_keepalive &&
				    ++skipped > HELPER_MASTER_MAX_SKIPPED_PKTS)
					return -EPROTO;
				continue;
			}
			vlog("master_rpc: type mismatch got=0x%08x expected=0x%08x rsp_len=%u",
			     got_type, rsp_type, pkt_len);
			free(pkt);
			return -EPROTO;
		}
		if (expect_msgid) {
			uint32_t got_msgid;
			if (pkt_len < 4) {
				free(pkt);
				return -EPROTO;
			}
			got_msgid = get32be(pkt);
			if (got_msgid != req_msgid) {
				vlog("master_rpc: skipped stale response type=0x%08x msgid=%u expected=%u len=%u",
				     got_type, got_msgid, req_msgid, pkt_len);
				free(pkt);
				pkt = NULL;
				if (++skipped > HELPER_MASTER_MAX_SKIPPED_PKTS)
					return -EPROTO;
				continue;
			}
		}
		*rsp_data = pkt;
		*rsp_len = pkt_len;
		return 0;
	}
}

static void *master_thread_main(void *arg)
{
	struct helper_session *s = arg;

	for (;;) {
		bool have_req = false;
		uint32_t req_type = 0;
		uint32_t rsp_type = 0;
		uint32_t req_len = 0;
		uint8_t *req_data = NULL;
		int ret;
		uint8_t *rsp_data = NULL;
		uint32_t rsp_len = 0;

		pthread_mutex_lock(&s->master_mu);
		while (!s->master_thread_stop && !s->master_req_pending) {
			struct timespec ts;

			clock_gettime(CLOCK_REALTIME, &ts);
			ts.tv_nsec += 250 * 1000 * 1000;
			if (ts.tv_nsec >= 1000000000L) {
				ts.tv_sec += 1;
				ts.tv_nsec -= 1000000000L;
			}
			pthread_cond_timedwait(&s->master_cv, &s->master_mu, &ts);
			if (!s->master_req_pending)
				break;
		}
		if (s->master_thread_stop) {
			pthread_mutex_unlock(&s->master_mu);
			break;
		}
		if (s->master_req_pending) {
			have_req = true;
			req_type = s->master_req_type;
			rsp_type = s->master_rsp_type;
			req_len = s->master_req_len;
			req_data = s->master_req_data;
		}
		pthread_mutex_unlock(&s->master_mu);

		if (!have_req) {
			ret = master_idle_pump(s);
			if (ret < 0 && master_conn_broken(ret)) {
				vlog("master idle: connection broken ret=%d, attempting re-register", ret);
				(void)session_reregister(s);
			}
			continue;
		}

		ret = master_rpc_once(s, req_type, req_data, req_len, rsp_type,
				      &rsp_data, &rsp_len);
		if (master_conn_broken(ret)) {
			vlog("master thread: connection broken ret=%d, attempting re-register", ret);
			ret = session_reregister(s);
			if (!ret) {
				ret = master_rpc_once(s, req_type, req_data, req_len,
						      rsp_type, &rsp_data, &rsp_len);
			}
		}

		pthread_mutex_lock(&s->master_mu);
		s->master_rsp_status = ret;
		s->master_rsp_data = rsp_data;
		s->master_rsp_len = rsp_len;
		s->master_req_pending = false;
		s->master_req_done = true;
		pthread_cond_broadcast(&s->master_cv);
		pthread_mutex_unlock(&s->master_mu);
	}

	return NULL;
}

static int session_start_master_thread(struct helper_session *s)
{
	int ret;

	pthread_mutex_lock(&s->master_mu);
	session_clear_master_slot(s);
	s->master_thread_stop = false;
	pthread_mutex_unlock(&s->master_mu);

	ret = pthread_create(&s->master_thread, NULL, master_thread_main, s);
	if (ret)
		return -ret;
	s->master_thread_running = true;
	return 0;
}

static void session_stop_master_thread(struct helper_session *s)
{
	if (!s->master_thread_running)
		return;

	pthread_mutex_lock(&s->master_mu);
	s->master_thread_stop = true;
	pthread_cond_broadcast(&s->master_cv);
	pthread_mutex_unlock(&s->master_mu);

	pthread_join(s->master_thread, NULL);
	s->master_thread_running = false;

	pthread_mutex_lock(&s->master_mu);
	session_clear_master_slot(s);
	pthread_mutex_unlock(&s->master_mu);
}

static int master_rpc(struct helper_session *s,
		      uint32_t req_type, const uint8_t *req_data, uint32_t req_len,
		      uint32_t rsp_type, uint8_t **rsp_data, uint32_t *rsp_len)
{
	uint8_t *req_copy = NULL;
	int ret;

	if (!rsp_data || !rsp_len)
		return -EINVAL;
	*rsp_data = NULL;
	*rsp_len = 0;

	if (!s->master_thread_running)
		return master_rpc_once(s, req_type, req_data, req_len, rsp_type,
				       rsp_data, rsp_len);

	if (req_len) {
		req_copy = malloc(req_len);
		if (!req_copy)
			return -ENOMEM;
		memcpy(req_copy, req_data, req_len);
	}

	pthread_mutex_lock(&s->master_mu);
	while (s->master_req_pending)
		pthread_cond_wait(&s->master_cv, &s->master_mu);

	session_clear_master_slot(s);
	s->master_req_type = req_type;
	s->master_req_data = req_copy;
	s->master_req_len = req_len;
	s->master_rsp_type = rsp_type;
	s->master_req_pending = true;
	s->master_req_done = false;
	pthread_cond_broadcast(&s->master_cv);

	while (!s->master_req_done)
		pthread_cond_wait(&s->master_cv, &s->master_mu);

	ret = s->master_rsp_status;
	*rsp_data = s->master_rsp_data;
	*rsp_len = s->master_rsp_len;
	s->master_rsp_data = NULL;
	s->master_rsp_len = 0;
	free(s->master_req_data);
	s->master_req_data = NULL;
	s->master_req_len = 0;
	s->master_req_done = false;
	pthread_cond_broadcast(&s->master_cv);
	pthread_mutex_unlock(&s->master_mu);

	return ret;
}

static void *read_prefetch_thread_main(void *arg)
{
	struct helper_session *s = arg;

	for (;;) {
		struct read_prefetch_request work = {0};
		struct chunk_meta meta;
		int ret;
		size_t i;

		pthread_mutex_lock(&s->read_prefetch_mu);
		for (;;) {
			if (s->read_prefetch_thread_stop)
				break;
			for (i = 0; i < HELPER_READ_PREFETCH_QUEUE_MAX; i++) {
				if (s->read_prefetch_queue[i].valid) {
					work = s->read_prefetch_queue[i];
					s->read_prefetch_queue[i].valid = false;
					break;
				}
			}
			if (work.valid || s->read_prefetch_thread_stop)
				break;
			pthread_cond_wait(&s->read_prefetch_cv, &s->read_prefetch_mu);
		}
		if (s->read_prefetch_thread_stop) {
			pthread_mutex_unlock(&s->read_prefetch_mu);
			break;
		}
		pthread_mutex_unlock(&s->read_prefetch_mu);

		if (!session_lookup_read_meta_cache(s, work.inode, work.chunk_idx, &meta, &ret)) {
			ret = session_read_chunk_meta(s, work.inode, work.chunk_idx, &meta);
			if (ret == MFS_ERROR_NOCHUNKSERVERS || ret == MFS_ERROR_NOCHUNK) {
				session_store_read_meta_cache(s, work.inode, work.chunk_idx, ret, NULL);
				continue;
			}
			if (ret)
				continue;
			session_store_read_meta_cache(s, work.inode, work.chunk_idx, 0, &meta);
		} else if (ret) {
			continue;
		}

		if (work.fetch_len > 0 && meta.loc_count > 0) {
			uint8_t *buf = malloc(work.fetch_len);
			uint32_t got = 0;
			struct read_prefetch_data_entry *victim = NULL;

			if (!buf)
				continue;
			pthread_mutex_lock(&s->read_prefetch_mu);
			while (!s->read_prefetch_thread_stop &&
			       s->read_foreground_inflight > 0)
				pthread_cond_wait(&s->read_prefetch_cv, &s->read_prefetch_mu);
			pthread_mutex_unlock(&s->read_prefetch_mu);
			if (s->read_prefetch_thread_stop) {
				free(buf);
				break;
			}
			ret = cs_read_data(&meta, work.chunk_off, work.fetch_len, buf, &got);
			if (!ret && got > 0) {
				pthread_mutex_lock(&s->read_prefetch_mu);
				for (i = 0; i < HELPER_READ_PREFETCH_DATA_CACHE_MAX; i++) {
					struct read_prefetch_data_entry *ent = &s->read_prefetch_data[i];

					if (ent->valid &&
					    ent->inode == work.inode &&
					    ent->chunk_idx == work.chunk_idx &&
					    ent->base == work.chunk_off) {
						victim = ent;
						break;
					}
					if (!victim || !victim->valid || ent->stamp < victim->stamp)
						victim = ent;
				}
				if (victim) {
					if (victim->cap < got) {
						uint8_t *tmp = realloc(victim->buf, got);
						if (tmp) {
							victim->buf = tmp;
							victim->cap = got;
						}
					}
					if (victim->cap >= got) {
						memcpy(victim->buf, buf, got);
						victim->valid = true;
						victim->inode = work.inode;
						victim->chunk_idx = work.chunk_idx;
						victim->base = work.chunk_off;
						victim->len = got;
						victim->stamp = ++s->read_prefetch_data_clock;
					}
				}
				pthread_mutex_unlock(&s->read_prefetch_mu);
			}
			free(buf);
		}
	}

	return NULL;
}

static int session_start_read_prefetch_thread(struct helper_session *s)
{
	size_t i;

	pthread_mutex_lock(&s->read_prefetch_mu);
	s->read_prefetch_thread_stop = false;
	memset(s->read_prefetch_queue, 0, sizeof(s->read_prefetch_queue));
	pthread_mutex_unlock(&s->read_prefetch_mu);

	for (i = 0; i < HELPER_READ_PREFETCH_WORKERS; i++) {
		int ret = pthread_create(&s->read_prefetch_threads[i], NULL,
					 read_prefetch_thread_main, s);
		if (ret) {
			pthread_mutex_lock(&s->read_prefetch_mu);
			s->read_prefetch_thread_stop = true;
			pthread_cond_broadcast(&s->read_prefetch_cv);
			pthread_mutex_unlock(&s->read_prefetch_mu);
			while (i-- > 0)
				pthread_join(s->read_prefetch_threads[i], NULL);
			return -ret;
		}
	}
	s->read_prefetch_thread_running = true;
	return 0;
}

static void session_stop_read_prefetch_thread(struct helper_session *s)
{
	if (!s->read_prefetch_thread_running)
		return;

	pthread_mutex_lock(&s->read_prefetch_mu);
	s->read_prefetch_thread_stop = true;
	memset(s->read_prefetch_queue, 0, sizeof(s->read_prefetch_queue));
	pthread_cond_broadcast(&s->read_prefetch_cv);
	pthread_mutex_unlock(&s->read_prefetch_mu);

	for (size_t i = 0; i < HELPER_READ_PREFETCH_WORKERS; i++)
		pthread_join(s->read_prefetch_threads[i], NULL);
	s->read_prefetch_thread_running = false;
}

static uint32_t next_msgid(struct helper_session *s)
{
	if (++s->next_msgid == 0)
		s->next_msgid = 1;
	return s->next_msgid;
}

static int parse_simple_status(uint32_t msgid, const uint8_t *rsp, uint32_t rsp_len,
			       uint8_t *status)
{
	if (rsp_len < 5)
		return -EPROTO;
	if (get32be(rsp) != msgid)
		return -EPROTO;
	*status = rsp[4];
	return 0;
}

static int parse_msgid(uint32_t msgid, const uint8_t *rsp, uint32_t rsp_len)
{
	if (rsp_len < 4)
		return -EPROTO;
	if (get32be(rsp) != msgid)
		return -EPROTO;
	return 0;
}

static int session_getattr(struct helper_session *s, uint32_t inode,
			   uint32_t uid, uint32_t gid,
			   struct mfs_wire_attr *out)
{
	uint8_t req[4 + 4 + 1 + 4 + 4];
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint32_t msgid = next_msgid(s);
	uint8_t status;
	uint64_t now_ms;
	struct getattr_cache_entry *slot = NULL;
	bool leader = false;
	int ret;
	size_t i;

	now_ms = monotonic_ms();
	pthread_mutex_lock(&s->getattr_cache_mu);
	for (;;) {
		slot = NULL;
		for (i = 0; i < HELPER_GETATTR_CACHE_MAX; i++) {
			struct getattr_cache_entry *ent = &s->getattr_cache[i];

			if (!ent->valid)
				continue;
			if (ent->inode != inode || ent->uid != uid || ent->gid != gid)
				continue;
			slot = ent;
			break;
		}
		if (slot && !slot->pending && slot->expires_ms >= now_ms) {
			slot->stamp = ++s->getattr_cache_clock;
			*out = slot->attr;
			pthread_mutex_unlock(&s->getattr_cache_mu);
			return 0;
		}
		if (slot && slot->pending) {
			pthread_cond_wait(&s->getattr_cache_cv, &s->getattr_cache_mu);
			now_ms = monotonic_ms();
			continue;
		}
		if (!slot) {
			struct getattr_cache_entry *victim = NULL;

			for (i = 0; i < HELPER_GETATTR_CACHE_MAX; i++) {
				struct getattr_cache_entry *ent = &s->getattr_cache[i];

				if (!victim || !victim->valid ||
				    (!ent->valid) ||
				    (ent->valid && ent->stamp < victim->stamp)) {
					victim = ent;
					if (!ent->valid)
						break;
				}
			}
			slot = victim;
		}
		if (slot) {
			memset(slot, 0, sizeof(*slot));
			slot->valid = true;
			slot->pending = true;
			slot->inode = inode;
			slot->uid = uid;
			slot->gid = gid;
			slot->stamp = ++s->getattr_cache_clock;
			leader = true;
		}
		break;
	}
	pthread_mutex_unlock(&s->getattr_cache_mu);

	vlog("getattr: inode=%u uid=%u gid=%u msgid=%u session=%u",
	     inode, uid, gid, msgid, s->session_id);

	put32be(req, msgid);
	put32be(req + 4, inode);
	req[8] = 0;
	put32be(req + 9, uid);
	put32be(req + 13, gid);

	ret = master_rpc(s, CLTOMA_FUSE_GETATTR, req, sizeof(req),
			 MATOCL_FUSE_GETATTR, &rsp, &rsp_len);
	if (ret) {
		vlog("getattr: master_rpc failed ret=%d", ret);
		if (leader) {
			pthread_mutex_lock(&s->getattr_cache_mu);
			if (slot)
				memset(slot, 0, sizeof(*slot));
			pthread_cond_broadcast(&s->getattr_cache_cv);
			pthread_mutex_unlock(&s->getattr_cache_mu);
		}
		return ret;
	}

	vlog("getattr: rsp_len=%u", rsp_len);

	if (rsp_len == 5) {
		ret = parse_simple_status(msgid, rsp, rsp_len, &status);
		free(rsp);
		if (ret) {
			vlog("getattr: parse_simple_status failed ret=%d", ret);
			if (leader) {
				pthread_mutex_lock(&s->getattr_cache_mu);
				if (slot)
					memset(slot, 0, sizeof(*slot));
				pthread_cond_broadcast(&s->getattr_cache_cv);
				pthread_mutex_unlock(&s->getattr_cache_mu);
			}
			return ret;
		}
		vlog("getattr: master returned status=%u (MFS error)", (unsigned)status);
		if (leader) {
			pthread_mutex_lock(&s->getattr_cache_mu);
			if (slot)
				memset(slot, 0, sizeof(*slot));
			pthread_cond_broadcast(&s->getattr_cache_cv);
			pthread_mutex_unlock(&s->getattr_cache_mu);
		}
		return status;
	}

	ret = parse_msgid(msgid, rsp, rsp_len);
	if (ret) {
		free(rsp);
		vlog("getattr: parse_msgid failed ret=%d", ret);
		if (leader) {
			pthread_mutex_lock(&s->getattr_cache_mu);
			if (slot)
				memset(slot, 0, sizeof(*slot));
			pthread_cond_broadcast(&s->getattr_cache_cv);
			pthread_mutex_unlock(&s->getattr_cache_mu);
		}
		return ret;
	}

	ret = parse_attr_record(rsp + 4, rsp_len - 4, out, NULL);
	if (!ret)
		s->attr_size = (rsp_len - 4 >= 36) ? 36 : 35;
	vlog("getattr: parse_attr_record ret=%d rsp_len=%u attr_size=%d",
	     ret, rsp_len, s->attr_size);
	free(rsp);
	if (leader) {
		pthread_mutex_lock(&s->getattr_cache_mu);
		if (!ret) {
			slot->valid = true;
			slot->pending = false;
			slot->inode = inode;
			slot->uid = uid;
			slot->gid = gid;
			slot->attr = *out;
			slot->expires_ms = monotonic_ms() + HELPER_GETATTR_CACHE_TTL_MS;
			slot->stamp = ++s->getattr_cache_clock;
		} else if (slot) {
			memset(slot, 0, sizeof(*slot));
		}
		pthread_cond_broadcast(&s->getattr_cache_cv);
		pthread_mutex_unlock(&s->getattr_cache_mu);
	}
	return ret;
}

static int session_get_register_random(struct helper_session *s, uint8_t rnd[32])
{
	uint8_t pkt[64 + 1];
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	int ret;

	memcpy(pkt, FUSE_REGISTER_BLOB_ACL, 64);
	pkt[64] = REGISTER_GETRANDOM;

	ret = master_rpc_once(s, CLTOMA_FUSE_REGISTER, pkt, sizeof(pkt),
			      MATOCL_FUSE_REGISTER, &rsp, &rsp_len);
	if (ret)
		return ret;
	if (rsp_len != 32) {
		free(rsp);
		return -EPROTO;
	}
	memcpy(rnd, rsp, 32);
	free(rsp);
	return 0;
}

static int session_register(struct helper_session *s,
			    const struct mfs_ctrl_register_req *req,
			    const uint8_t *tail, uint32_t tail_len,
			    struct mfs_ctrl_register_rsp *out)
{
	uint8_t *pkt = NULL;
	uint32_t pkt_len;
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint8_t rnd[32];
	uint8_t digest[16];
	uint8_t passdigest[16];
	uint32_t v1, v2;
	int fd;
	int ret;
	size_t off;
	const uint8_t *password = NULL;
	md5ctx ctx;

	if (tail_len < (uint32_t)req->master_len + req->subdir_len + req->password_len)
		return -EINVAL;

	session_stop_master_thread(s);
	session_reset_state(s);
	memcpy(s->master_host, tail, req->master_len);
	s->master_host[req->master_len] = '\0';
	off = req->master_len;
	memcpy(s->subdir, tail + off, req->subdir_len);
	s->subdir[req->subdir_len] = '\0';
	password = tail + off + req->subdir_len;
	s->master_port = req->master_port ? req->master_port : MFS_DEFAULT_MASTER_PORT;

	fprintf(stderr, "register: master_host='%s' master_port=%u master_len=%u subdir='%s' subdir_len=%u password_len=%u tail_len=%u\n",
	     s->master_host, (unsigned)s->master_port,
	     (unsigned)req->master_len, s->subdir,
	     (unsigned)req->subdir_len, (unsigned)req->password_len,
	     (unsigned)tail_len);

	fd = connect_tcp(s->master_host, s->master_port, HELPER_MASTER_TIMEOUT_MS);
	if (fd < 0) {
		fprintf(stderr, "register: connect_tcp failed ret=%d host='%s' port=%u\n",
		     fd, s->master_host, (unsigned)s->master_port);
		return fd;
	}
	set_socket_timeouts(fd, HELPER_MASTER_TIMEOUT_MS);
	s->master_fd = fd;

	/*
	 * MooseFS wire protocol: info and subdir string lengths INCLUDE
	 * the null terminator (see mfsclient/mastercomm.c: strlen()+1).
	 * The kernel module sends lengths WITHOUT null (strnlen), so we
	 * add +1 here for each string when building the registration packet.
	 */
	{
		uint32_t info_len = (uint32_t)strlen(HELPER_INFO_STR) + 1; /* +1 for NUL */
		uint32_t sub_len = req->subdir_len ? req->subdir_len + 1 : 2; /* "/" + NUL */

		pkt_len = 64 + 1 + 4 + 4 + info_len + 4 + sub_len +
			  (req->password_len ? 16U : 0U);
		pkt = calloc(1, pkt_len);
		if (!pkt) {
			close(fd);
			s->master_fd = -1;
			return -ENOMEM;
		}
		off = 0;
		memcpy(pkt + off, FUSE_REGISTER_BLOB_ACL, 64);
		off += 64;
		pkt[off++] = REGISTER_NEWSESSION;
		/* Version: 2 bytes major, 1 byte mid, 1 byte minor (matches mfsmount) */
		put16be(pkt + off, MFS_VERSMAJ);
		off += 2;
		pkt[off++] = MFS_VERSMID;
		pkt[off++] = MFS_VERSMIN;
		/* info string with null terminator */
		put32be(pkt + off, info_len);
		off += 4;
		memcpy(pkt + off, HELPER_INFO_STR, info_len); /* includes NUL */
		off += info_len;
		/* subdir string with null terminator */
		put32be(pkt + off, sub_len);
		off += 4;
		if (req->subdir_len) {
			memcpy(pkt + off, s->subdir, req->subdir_len);
			pkt[off + req->subdir_len] = '\0'; /* explicit NUL */
		} else {
			pkt[off] = '/';  /* default: root subdir */
			/* calloc already zeroed the rest */
		}
		off += sub_len;
		if (req->password_len) {
			ret = session_get_register_random(s, rnd);
			if (ret) {
				free(pkt);
				close(fd);
				s->master_fd = -1;
				return ret;
			}
			md5_init(&ctx);
			md5_update(&ctx, password, req->password_len);
			md5_final(passdigest, &ctx);
			md5_init(&ctx);
			md5_update(&ctx, rnd, 16);
			md5_update(&ctx, passdigest, 16);
			md5_update(&ctx, rnd + 16, 16);
			md5_final(digest, &ctx);
			memcpy(pkt + off, digest, 16);
		}
	}

	ret = master_rpc_once(s, CLTOMA_FUSE_REGISTER, pkt, pkt_len,
			      MATOCL_FUSE_REGISTER, &rsp, &rsp_len);
	free(pkt);
	if (ret)
		goto fail;

	if (rsp_len == 1) {
		ret = rsp[0];
		free(rsp);
		goto fail;
	}
	if (rsp_len < 8) {
		free(rsp);
		ret = -EPROTO;
		goto fail;
	}

	v1 = get32be(rsp);
	v2 = get32be(rsp + 4);
	if (v1 >= 0x00010000 && v1 <= 0x0fffffff) {
		s->master_version = v1;
		s->session_id = v2;
	} else {
		s->master_version = 0;
		s->session_id = v1;
	}
	free(rsp);

	s->active = true;
	ret = session_start_master_thread(s);
	if (ret)
		goto fail;

	ret = session_getattr(s, MFS_ROOT_ID, req->mount_uid, req->mount_gid,
			      &out->root_attr);
	if (ret)
		goto fail;

	out->session_id = s->session_id;
	out->root_inode = MFS_ROOT_ID;
	s->active = true;
	vlog("registered session=%u master=%s:%u master_version=0x%08x",
	     s->session_id, s->master_host, (unsigned)s->master_port,
	     s->master_version);
	return 0;

fail:
	session_stop_master_thread(s);
	if (s->master_fd >= 0)
		close(s->master_fd);
	s->master_fd = -1;
	s->active = false;
	return ret;
}

static int session_lookup(struct helper_session *s,
			 const struct mfs_ctrl_lookup_req *req,
			 const uint8_t *name,
			 struct mfs_ctrl_lookup_rsp *out)
{
	uint8_t *pkt;
	uint32_t pkt_len;
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint32_t msgid = next_msgid(s);
	uint8_t status;
	int ret;

	if (req->name_len == 0 || req->name_len > MFS_NAME_MAX)
		return MFS_ERROR_EINVAL;

	pkt_len = 4 + 4 + 1 + req->name_len + 4 + 4 + 4;
	pkt = malloc(pkt_len);
	if (!pkt)
		return -ENOMEM;
	put32be(pkt, msgid);
	put32be(pkt + 4, req->parent_inode);
	pkt[8] = req->name_len;
	memcpy(pkt + 9, name, req->name_len);
	put32be(pkt + 9 + req->name_len, req->uid);
	put32be(pkt + 13 + req->name_len, 1);
	put32be(pkt + 17 + req->name_len, req->gid);

	ret = master_rpc(s, CLTOMA_FUSE_LOOKUP, pkt, pkt_len,
			 MATOCL_FUSE_LOOKUP, &rsp, &rsp_len);
	free(pkt);
	if (ret)
		return ret;

	if (rsp_len == 5) {
		ret = parse_simple_status(msgid, rsp, rsp_len, &status);
		free(rsp);
		if (ret)
			return ret;
		return status;
	}

	ret = parse_msgid(msgid, rsp, rsp_len);
	if (ret) {
		free(rsp);
		return ret;
	}
	if (rsp_len < 8 + 35) {
		free(rsp);
		return -EPROTO;
	}
	out->inode = get32be(rsp + 4);
	ret = parse_attr_record(rsp + 8, rsp_len - 8, &out->attr, NULL);
	free(rsp);
	return ret;
}

static int session_setattr(struct helper_session *s,
			  const struct mfs_ctrl_setattr_req *req,
			  struct mfs_wire_attr *out)
{
	uint8_t pkt[4 + 4 + 1 + 4 + 4 + 4 + 1 + 2 + 4 + 4 + 4 + 4 + 1 + 1];
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint32_t msgid = next_msgid(s);
	uint8_t status;
	uint8_t setmask = 0;
	uint32_t pkt_len = 0;
	uint32_t atime = (uint32_t)(req->atime_ns / 1000000000ULL);
	uint32_t mtime = (uint32_t)(req->mtime_ns / 1000000000ULL);
	int ret;

	if (req->valid & MFS_SETATTR_MODE)
		setmask |= SET_MODE_FLAG;
	if (req->valid & MFS_SETATTR_UID)
		setmask |= SET_UID_FLAG;
	if (req->valid & MFS_SETATTR_GID)
		setmask |= SET_GID_FLAG;
	if (req->valid & MFS_SETATTR_ATIME_NOW)
		setmask |= SET_ATIME_NOW_FLAG;
	else if (req->valid & MFS_SETATTR_ATIME)
		setmask |= SET_ATIME_FLAG;
	if (req->valid & MFS_SETATTR_MTIME_NOW)
		setmask |= SET_MTIME_NOW_FLAG;
	else if (req->valid & MFS_SETATTR_MTIME)
		setmask |= SET_MTIME_FLAG;

	vlog("setattr: inode=%u valid=0x%x setmask=0x%x mode=%o uid=%u gid=%u atime=%u mtime=%u",
	     req->inode, req->valid, setmask, req->mode & 07777,
	     req->attr_uid, req->attr_gid, atime, mtime);

	put32be(pkt, msgid);
	put32be(pkt + 4, req->inode);
	pkt[8] = 0;
	put32be(pkt + 9, req->uid);
	put32be(pkt + 13, 1);
	put32be(pkt + 17, req->gid);
	pkt[21] = setmask;
	put16be(pkt + 22, (uint16_t)(req->mode & 07777));
	put32be(pkt + 24, req->attr_uid);
	put32be(pkt + 28, req->attr_gid);
	put32be(pkt + 32, atime);
	put32be(pkt + 36, mtime);
	pkt[40] = 0;
	pkt[41] = 0;

	if (s->master_version < VERSION2INT(1, 6, 25))
		pkt_len = 35;
	else if (s->master_version < VERSION2INT(1, 6, 28))
		pkt_len = 36;
	else if (s->master_version < VERSION2INT(2, 0, 0))
		pkt_len = 37;
	else if (s->master_version < VERSION2INT(3, 0, 93) ||
		 s->master_version == VERSION2INT(4, 0, 0) ||
		 s->master_version == VERSION2INT(4, 0, 1))
		pkt_len = 41;
	else
		pkt_len = 42;

	vlog("setattr: master_version=0x%08x pkt_len=%u",
	     s->master_version, pkt_len);

	ret = master_rpc(s, CLTOMA_FUSE_SETATTR, pkt, pkt_len,
			 MATOCL_FUSE_SETATTR, &rsp, &rsp_len);
	if (ret)
		return ret;

	if (rsp_len == 5) {
		ret = parse_simple_status(msgid, rsp, rsp_len, &status);
		free(rsp);
		if (ret)
			return ret;
		return status;
	}

	ret = parse_msgid(msgid, rsp, rsp_len);
	if (!ret)
		ret = parse_attr_record(rsp + 4, rsp_len - 4, out, NULL);
	free(rsp);
	return ret;
}

static int session_truncate(struct helper_session *s,
			   const struct mfs_ctrl_truncate_req *req,
			   struct mfs_wire_attr *out)
{
	uint8_t pkt[4 + 4 + 1 + 4 + 4 + 4 + 8];
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint32_t msgid = next_msgid(s);
	uint8_t status;
	int ret;

	put32be(pkt, msgid);
	put32be(pkt + 4, req->inode);
	pkt[8] = 0;
	put32be(pkt + 9, req->uid);
	put32be(pkt + 13, 1);
	put32be(pkt + 17, req->gid);
	put64be(pkt + 21, req->size);

	ret = master_rpc(s, CLTOMA_FUSE_TRUNCATE, pkt, sizeof(pkt),
			 MATOCL_FUSE_TRUNCATE, &rsp, &rsp_len);
	if (ret)
		return ret;

	if (rsp_len == 5) {
		ret = parse_simple_status(msgid, rsp, rsp_len, &status);
		free(rsp);
		if (ret)
			return ret;
		return status;
	}

	ret = parse_msgid(msgid, rsp, rsp_len);
	if (ret) {
		free(rsp);
		return ret;
	}
	if (rsp_len >= 4 + 8 + 35)
		ret = parse_attr_record(rsp + 12, rsp_len - 12, out, NULL);
	else
		ret = parse_attr_record(rsp + 4, rsp_len - 4, out, NULL);
	free(rsp);
	return ret;
}

struct parsed_dirent {
	uint32_t inode;
	uint8_t type;
	uint16_t name_len;
	char *name;
};

static void free_parsed_dirents(struct parsed_dirent *ents, size_t n)
{
	size_t i;
	for (i = 0; i < n; i++)
		free(ents[i].name);
	free(ents);
}

static int parse_readdir_entries(const uint8_t *buf, size_t len,
				 struct parsed_dirent **out_ents,
				 size_t *out_count)
{
	size_t off;
	size_t cap = 0;
	size_t cnt = 0;
	struct parsed_dirent *ents = NULL;

	if (len < 4)
		return -EPROTO;

	for (off = 0; off < len;) {
		uint8_t nl;
		struct parsed_dirent de;
		size_t c;
		struct mfs_wire_attr attr;

		nl = buf[off++];
		if (off + nl + 4 > len) {
			free_parsed_dirents(ents, cnt);
			return -EPROTO;
		}
		memset(&de, 0, sizeof(de));
		de.name_len = nl;
		de.name = malloc(nl);
		if (!de.name) {
			free_parsed_dirents(ents, cnt);
			return -ENOMEM;
		}
		memcpy(de.name, buf + off, nl);
		off += nl;
		de.inode = get32be(buf + off);
		off += 4;

		if (parse_attr_record(buf + off, len - off, &attr, &c) != 0) {
			free(de.name);
			free_parsed_dirents(ents, cnt);
			return -EPROTO;
		}
		de.type = attr.type;
		off += c;

		if (cnt == cap) {
			size_t ncap = cap ? cap * 2 : 64;
			struct parsed_dirent *tmp = realloc(ents, ncap * sizeof(*tmp));
			if (!tmp) {
				free(de.name);
				free_parsed_dirents(ents, cnt);
				return -ENOMEM;
			}
			ents = tmp;
			cap = ncap;
		}
		ents[cnt++] = de;
	}

	*out_ents = ents;
	*out_count = cnt;
	return 0;
}

static int session_readdir(struct helper_session *s,
			  const struct mfs_ctrl_readdir_req *req,
			  uint8_t **out, uint32_t *out_len)
{
	uint8_t pkt[4 + 4 + 4 + 4 + 4 + 1 + 4 + 8];
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint32_t msgid = next_msgid(s);
	uint8_t status;
	struct parsed_dirent *ents = NULL;
	size_t ent_count = 0;
	size_t start;
	size_t maxe;
	size_t i;
	uint32_t payload_len;
	uint8_t *payload;
	struct mfs_ctrl_readdir_rsp *rr;
	uint8_t *p;
	int ret;

	put32be(pkt, msgid);
	put32be(pkt + 4, req->inode);
	put32be(pkt + 8, req->uid);
	put32be(pkt + 12, 1);
	put32be(pkt + 16, req->gid);
	pkt[20] = GETDIR_FLAG_WITHATTR;
	put32be(pkt + 21, 100000);
	put64be(pkt + 25, 0);

	ret = master_rpc(s, CLTOMA_FUSE_READDIR, pkt, sizeof(pkt),
			 MATOCL_FUSE_READDIR, &rsp, &rsp_len);
	if (ret)
		return ret;

	if (rsp_len == 5) {
		ret = parse_simple_status(msgid, rsp, rsp_len, &status);
		free(rsp);
		if (ret)
			return ret;
		return status;
	}

	ret = parse_msgid(msgid, rsp, rsp_len);
	if (ret) {
		free(rsp);
		return ret;
	}

	if (rsp_len < 4 + 8) {
		free(rsp);
		return -EPROTO;
	}

	ret = parse_readdir_entries(rsp + 12, rsp_len - 12, &ents, &ent_count);
	if (ret) {
		ret = parse_readdir_entries(rsp + 4, rsp_len - 4, &ents, &ent_count);
		if (ret) {
			free(rsp);
			return ret;
		}
	}
	free(rsp);

	start = (req->offset > 2) ? (size_t)(req->offset - 2) : 0;
	if (start > ent_count)
		start = ent_count;
	maxe = req->max_entries ? req->max_entries : 256;
	if (maxe > ent_count - start)
		maxe = ent_count - start;

	payload_len = sizeof(*rr);
	for (i = 0; i < maxe; i++)
		payload_len += sizeof(struct mfs_ctrl_dirent_wire) + ents[start + i].name_len;
	payload = calloc(1, payload_len);
	if (!payload) {
		free_parsed_dirents(ents, ent_count);
		return -ENOMEM;
	}

	rr = (struct mfs_ctrl_readdir_rsp *)payload;
	rr->count = (uint32_t)maxe;
	rr->eof = (start + maxe >= ent_count) ? 1 : 0;
	rr->next_offset = (uint64_t)(start + maxe + 2);
	p = payload + sizeof(*rr);
	for (i = 0; i < maxe; i++) {
		struct mfs_ctrl_dirent_wire *w = (struct mfs_ctrl_dirent_wire *)p;
		const struct parsed_dirent *de = &ents[start + i];
		w->inode = de->inode;
		w->type = de->type;
		w->name_len = de->name_len;
		w->next_offset = (uint64_t)(start + i + 3);
		p += sizeof(*w);
		memcpy(p, de->name, de->name_len);
		p += de->name_len;
	}

	free_parsed_dirents(ents, ent_count);
	*out = payload;
	*out_len = payload_len;
	return 0;
}

static int parse_create_like(uint32_t msgid, const uint8_t *rsp, uint32_t rsp_len,
			     struct mfs_ctrl_create_rsp *out)
{
	uint8_t status;
	int ret;

	if (rsp_len == 5) {
		ret = parse_simple_status(msgid, rsp, rsp_len, &status);
		if (ret)
			return ret;
		return status;
	}
	ret = parse_msgid(msgid, rsp, rsp_len);
	if (ret)
		return ret;

	if (rsp_len >= 4 + 1 + 4 + 35) {
		out->inode = get32be(rsp + 5);
		return parse_attr_record(rsp + 9, rsp_len - 9, &out->attr, NULL);
	}
	if (rsp_len >= 4 + 4 + 35) {
		out->inode = get32be(rsp + 4);
		return parse_attr_record(rsp + 8, rsp_len - 8, &out->attr, NULL);
	}
	return -EPROTO;
}

static int session_create(struct helper_session *s,
			 const struct mfs_ctrl_create_req *req,
			 const uint8_t *name,
			 struct mfs_ctrl_create_rsp *out)
{
	uint32_t msgid = next_msgid(s);
	uint32_t pkt_len = 4 + 4 + 1 + req->name_len + 2 + 2 + 4 + 4 + 4;
	uint8_t *pkt = malloc(pkt_len);
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	int ret;

	vlog("create: parent=%u name_len=%u mode=%o uid=%u gid=%u session=%u",
	     req->parent_inode, req->name_len, req->mode & 07777,
	     req->uid, req->gid, req->session_id);

	if (!pkt)
		return -ENOMEM;
	put32be(pkt, msgid);
	put32be(pkt + 4, req->parent_inode);
	pkt[8] = req->name_len;
	memcpy(pkt + 9, name, req->name_len);
	put16be(pkt + 9 + req->name_len, req->mode & 07777);
	put16be(pkt + 11 + req->name_len, 0);
	put32be(pkt + 13 + req->name_len, req->uid);
	put32be(pkt + 17 + req->name_len, 1);
	put32be(pkt + 21 + req->name_len, req->gid);

	ret = master_rpc(s, CLTOMA_FUSE_CREATE, pkt, pkt_len,
			 MATOCL_FUSE_CREATE, &rsp, &rsp_len);
	free(pkt);
	if (ret) {
		vlog("create: master_rpc failed ret=%d", ret);
		return ret;
	}
	ret = parse_create_like(msgid, rsp, rsp_len, out);
	vlog("create: parse_create_like ret=%d rsp_len=%u inode=%u",
	     ret, rsp_len, out->inode);
	free(rsp);
	return ret;
}

static int session_mkdir(struct helper_session *s,
			const struct mfs_ctrl_mkdir_req *req,
			const uint8_t *name,
			struct mfs_ctrl_create_rsp *out)
{
	uint32_t msgid = next_msgid(s);
	uint32_t pkt_len = 4 + 4 + 1 + req->name_len + 2 + 2 + 4 + 4 + 4 + 1;
	uint8_t *pkt = malloc(pkt_len);
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint8_t status;
	int ret;

	if (!pkt)
		return -ENOMEM;
	put32be(pkt, msgid);
	put32be(pkt + 4, req->parent_inode);
	pkt[8] = req->name_len;
	memcpy(pkt + 9, name, req->name_len);
	put16be(pkt + 9 + req->name_len, req->mode & 07777);
	put16be(pkt + 11 + req->name_len, 0);
	put32be(pkt + 13 + req->name_len, req->uid);
	put32be(pkt + 17 + req->name_len, 1);
	put32be(pkt + 21 + req->name_len, req->gid);
	pkt[25 + req->name_len] = 0;

	ret = master_rpc(s, CLTOMA_FUSE_MKDIR, pkt, pkt_len,
			 MATOCL_FUSE_MKDIR, &rsp, &rsp_len);
	free(pkt);
	if (ret)
		return ret;

	if (rsp_len == 5) {
		ret = parse_simple_status(msgid, rsp, rsp_len, &status);
		free(rsp);
		if (ret)
			return ret;
		return status;
	}
	ret = parse_msgid(msgid, rsp, rsp_len);
	if (ret) {
		free(rsp);
		return ret;
	}
	if (rsp_len < 4 + 4 + 35) {
		free(rsp);
		return -EPROTO;
	}
	out->inode = get32be(rsp + 4);
	ret = parse_attr_record(rsp + 8, rsp_len - 8, &out->attr, NULL);
	free(rsp);
	return ret;
}

static int session_unlink_rmdir(struct helper_session *s,
				const struct mfs_ctrl_unlink_req *req,
				const uint8_t *name,
				uint32_t req_type, uint32_t rsp_type)
{
	uint32_t msgid = next_msgid(s);
	uint32_t pkt_len = 4 + 4 + 1 + req->name_len + 4 + 4 + 4;
	uint8_t *pkt = malloc(pkt_len);
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint8_t status;
	int ret;

	if (!pkt)
		return -ENOMEM;
	put32be(pkt, msgid);
	put32be(pkt + 4, req->parent_inode);
	pkt[8] = req->name_len;
	memcpy(pkt + 9, name, req->name_len);
	put32be(pkt + 9 + req->name_len, req->uid);
	put32be(pkt + 13 + req->name_len, 1);
	put32be(pkt + 17 + req->name_len, req->gid);

	ret = master_rpc(s, req_type, pkt, pkt_len, rsp_type, &rsp, &rsp_len);
	free(pkt);
	if (ret)
		return ret;
	ret = parse_simple_status(msgid, rsp, rsp_len, &status);
	free(rsp);
	if (ret)
		return ret;
	return status;
}

static int session_link(struct helper_session *s,
			const struct mfs_ctrl_link_req *req,
			const uint8_t *name)
{
	uint32_t msgid = next_msgid(s);
	uint32_t pkt_len = 4 + 4 + 4 + 1 + req->name_len + 4 + 4 + 4;
	uint8_t *pkt = malloc(pkt_len);
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint8_t status;
	int ret;

	if (!pkt)
		return -ENOMEM;
	put32be(pkt, msgid);
	put32be(pkt + 4, req->inode);
	put32be(pkt + 8, req->new_parent_inode);
	pkt[12] = req->name_len;
	memcpy(pkt + 13, name, req->name_len);
	put32be(pkt + 13 + req->name_len, req->uid);
	put32be(pkt + 17 + req->name_len, 1);
	put32be(pkt + 21 + req->name_len, req->gid);

	ret = master_rpc(s, CLTOMA_FUSE_LINK, pkt, pkt_len,
			 MATOCL_FUSE_LINK, &rsp, &rsp_len);
	free(pkt);
	if (ret)
		return ret;
	if (rsp_len == 5) {
		ret = parse_simple_status(msgid, rsp, rsp_len, &status);
		free(rsp);
		if (ret)
			return ret;
		return status;
	}
	ret = parse_msgid(msgid, rsp, rsp_len);
	free(rsp);
	return ret;
}

static int session_rename(struct helper_session *s,
		      const struct mfs_ctrl_rename_req *req,
		      const uint8_t *old_name,
		      const uint8_t *new_name)
{
	uint32_t msgid = next_msgid(s);
	uint32_t pkt_len = 4 + 4 + 1 + req->old_name_len + 4 + 1 + req->new_name_len +
			   4 + 4 + 4 + 1;
	uint8_t *pkt = malloc(pkt_len);
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint8_t status;
	uint8_t mode = MFS_RENAME_STD;
	int ret;

	if (!pkt)
		return -ENOMEM;
	if (req->flags & 2)
		mode = MFS_RENAME_EXCHANGE;
	else if (req->flags & 1)
		mode = MFS_RENAME_NOREPLACE;

	put32be(pkt, msgid);
	put32be(pkt + 4, req->old_parent_inode);
	pkt[8] = req->old_name_len;
	memcpy(pkt + 9, old_name, req->old_name_len);
	put32be(pkt + 9 + req->old_name_len, req->new_parent_inode);
	pkt[13 + req->old_name_len] = req->new_name_len;
	memcpy(pkt + 14 + req->old_name_len, new_name, req->new_name_len);
	put32be(pkt + 14 + req->old_name_len + req->new_name_len, req->uid);
	put32be(pkt + 18 + req->old_name_len + req->new_name_len, 1);
	put32be(pkt + 22 + req->old_name_len + req->new_name_len, req->gid);
	pkt[26 + req->old_name_len + req->new_name_len] = mode;

	ret = master_rpc(s, CLTOMA_FUSE_RENAME, pkt, pkt_len,
			 MATOCL_FUSE_RENAME, &rsp, &rsp_len);
	free(pkt);
	if (ret)
		return ret;
	ret = parse_simple_status(msgid, rsp, rsp_len, &status);
	free(rsp);
	if (ret)
		return ret;
	return status;
}

static int session_readlink(struct helper_session *s,
			   const struct mfs_ctrl_inode_req *req,
			   uint8_t **out, uint32_t *out_len)
{
	uint8_t pkt[8];
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint32_t msgid = next_msgid(s);
	uint8_t status;
	struct mfs_ctrl_readlink_rsp *rr;
	uint8_t *payload;
	uint32_t plen;
	int ret;

	put32be(pkt, msgid);
	put32be(pkt + 4, req->inode);

	ret = master_rpc(s, CLTOMA_FUSE_READLINK, pkt, sizeof(pkt),
			 MATOCL_FUSE_READLINK, &rsp, &rsp_len);
	if (ret)
		return ret;
	if (rsp_len == 5) {
		ret = parse_simple_status(msgid, rsp, rsp_len, &status);
		free(rsp);
		if (ret)
			return ret;
		return status;
	}
	ret = parse_msgid(msgid, rsp, rsp_len);
	if (ret) {
		free(rsp);
		return ret;
	}
	if (rsp_len < 8) {
		free(rsp);
		return -EPROTO;
	}
	plen = get32be(rsp + 4);
	if (rsp_len < 8 + plen || plen > MFS_SYMLINK_MAX) {
		free(rsp);
		return -EPROTO;
	}

	payload = calloc(1, sizeof(*rr) + plen);
	if (!payload) {
		free(rsp);
		return -ENOMEM;
	}
	rr = (struct mfs_ctrl_readlink_rsp *)payload;
	rr->size = plen;
	memcpy(payload + sizeof(*rr), rsp + 8, plen);
	free(rsp);
	*out = payload;
	*out_len = sizeof(*rr) + plen;
	return 0;
}

static int session_symlink(struct helper_session *s,
			  const struct mfs_ctrl_symlink_req *req,
			  const uint8_t *name,
			  const uint8_t *target,
			  struct mfs_ctrl_create_rsp *out)
{
	uint32_t msgid = next_msgid(s);
	uint32_t pkt_len = 4 + 4 + 1 + req->name_len + 4 + req->target_len + 4 + 4 + 4;
	uint8_t *pkt = malloc(pkt_len);
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	int ret;

	if (!pkt)
		return -ENOMEM;
	put32be(pkt, msgid);
	put32be(pkt + 4, req->parent_inode);
	pkt[8] = req->name_len;
	memcpy(pkt + 9, name, req->name_len);
	put32be(pkt + 9 + req->name_len, req->target_len);
	memcpy(pkt + 13 + req->name_len, target, req->target_len);
	put32be(pkt + 13 + req->name_len + req->target_len, req->uid);
	put32be(pkt + 17 + req->name_len + req->target_len, 1);
	put32be(pkt + 21 + req->name_len + req->target_len, req->gid);

	ret = master_rpc(s, CLTOMA_FUSE_SYMLINK, pkt, pkt_len,
			 MATOCL_FUSE_SYMLINK, &rsp, &rsp_len);
	free(pkt);
	if (ret)
		return ret;

	ret = parse_create_like(msgid, rsp, rsp_len, out);
	free(rsp);
	return ret;
}

static int session_getxattr(struct helper_session *s,
			   const struct mfs_ctrl_xattr_req *req,
			   const uint8_t *name,
			   uint8_t **out, uint32_t *out_len)
{
	uint32_t msgid = next_msgid(s);
	uint32_t pkt_len = 4 + 4 + 1 + req->name_len + 1 + 1 + 4 + 4 + 4;
	uint8_t *pkt = malloc(pkt_len);
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint8_t status;
	uint8_t *payload;
	struct mfs_ctrl_xattr_rsp *xr;
	uint32_t vlen;
	int ret;

	if (!pkt)
		return -ENOMEM;
	put32be(pkt, msgid);
	put32be(pkt + 4, req->inode);
	pkt[8] = req->name_len;
	if (req->name_len)
		memcpy(pkt + 9, name, req->name_len);
	pkt[9 + req->name_len] = (req->flags & 1) ? 1 : 0;
	pkt[10 + req->name_len] = 0;
	put32be(pkt + 11 + req->name_len, req->uid);
	put32be(pkt + 15 + req->name_len, 1);
	put32be(pkt + 19 + req->name_len, req->gid);

	ret = master_rpc(s, CLTOMA_FUSE_GETXATTR, pkt, pkt_len,
			 MATOCL_FUSE_GETXATTR, &rsp, &rsp_len);
	free(pkt);
	if (ret)
		return ret;
	if (rsp_len == 5) {
		ret = parse_simple_status(msgid, rsp, rsp_len, &status);
		free(rsp);
		if (ret)
			return ret;
		return status;
	}

	ret = parse_msgid(msgid, rsp, rsp_len);
	if (ret) {
		free(rsp);
		return ret;
	}
	if (rsp_len < 8) {
		free(rsp);
		return -EPROTO;
	}
	vlen = get32be(rsp + 4);
	if (rsp_len < 8 + vlen) {
		free(rsp);
		return -EPROTO;
	}

	payload = calloc(1, sizeof(*xr) + vlen);
	if (!payload) {
		free(rsp);
		return -ENOMEM;
	}
	xr = (struct mfs_ctrl_xattr_rsp *)payload;
	xr->size = vlen;
	if (vlen)
		memcpy(payload + sizeof(*xr), rsp + 8, vlen);
	free(rsp);
	*out = payload;
	*out_len = sizeof(*xr) + vlen;
	return 0;
}

static int session_setxattr(struct helper_session *s,
			   const struct mfs_ctrl_xattr_req *req,
			   const uint8_t *name,
			   const uint8_t *value)
{
	uint32_t msgid = next_msgid(s);
	uint32_t pkt_len = 4 + 4 + 1 + req->name_len + 4 + req->value_len + 1 + 1 + 4 + 4 + 4;
	uint8_t *pkt = malloc(pkt_len);
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint8_t status;
	int ret;

	if (!pkt)
		return -ENOMEM;
	put32be(pkt, msgid);
	put32be(pkt + 4, req->inode);
	pkt[8] = req->name_len;
	memcpy(pkt + 9, name, req->name_len);
	put32be(pkt + 9 + req->name_len, req->value_len);
	if (req->value_len)
		memcpy(pkt + 13 + req->name_len, value, req->value_len);
	pkt[13 + req->name_len + req->value_len] = req->flags & 3;
	pkt[14 + req->name_len + req->value_len] = 0;
	put32be(pkt + 15 + req->name_len + req->value_len, req->uid);
	put32be(pkt + 19 + req->name_len + req->value_len, 1);
	put32be(pkt + 23 + req->name_len + req->value_len, req->gid);

	ret = master_rpc(s, CLTOMA_FUSE_SETXATTR, pkt, pkt_len,
			 MATOCL_FUSE_SETXATTR, &rsp, &rsp_len);
	free(pkt);
	if (ret)
		return ret;
	ret = parse_simple_status(msgid, rsp, rsp_len, &status);
	free(rsp);
	if (ret)
		return ret;
	return status;
}

static int session_fsync(struct helper_session *s, const struct mfs_ctrl_fsync_req *req)
{
	int ret;

	vlog("fsync: inode=%u datasync=%u session=%u",
	     req->inode, req->datasync, req->session_id);

	ret = session_finalize_active_write(s);
	if (ret) {
		vlog("fsync: finalize_active_write failed ret=%d", ret);
		return ret;
	}
	return MFS_STATUS_OK;
}

static int session_statfs(struct helper_session *s,
			  struct mfs_ctrl_statfs_rsp *out)
{
	uint8_t pkt[4];
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint32_t msgid = next_msgid(s);
	int ret;

	put32be(pkt, msgid);
	ret = master_rpc(s, CLTOMA_FUSE_STATFS, pkt, sizeof(pkt),
			 MATOCL_FUSE_STATFS, &rsp, &rsp_len);
	if (ret)
		return ret;
	if (rsp_len < 4 + 8 * 3 + 4) {
		free(rsp);
		return -EPROTO;
	}
	if (parse_msgid(msgid, rsp, rsp_len)) {
		free(rsp);
		return -EPROTO;
	}

	out->total_space = get64be(rsp + 4);
	out->avail_space = get64be(rsp + 12);
	out->trash_space = get64be(rsp + 20);
	if (rsp_len >= 4 + 8 * 4 + 4) {
		out->sustained_space = get64be(rsp + 28);
		out->inodes = get32be(rsp + 36);
	} else {
		out->sustained_space = 0;
		out->inodes = get32be(rsp + 28);
	}
	out->free_space = out->avail_space;
	free(rsp);
	return 0;
}

static int parse_chunk_meta_rsp(uint32_t msgid, const uint8_t *rsp, uint32_t rsp_len,
				struct chunk_meta *m)
{
	const uint8_t *p;
	const uint8_t *end;
	uint8_t status;
	size_t e_sz;

	if (rsp_len == 5) {
		if (parse_simple_status(msgid, rsp, rsp_len, &status))
			return -EPROTO;
		return status;
	}
	if (parse_msgid(msgid, rsp, rsp_len))
		return -EPROTO;

	memset(m, 0, sizeof(*m));
	p = rsp + 4;
	end = rsp + rsp_len;
	if ((size_t)(end - p) < 20)
		return -EPROTO;

	if ((end - p) >= 21 && (p[0] >= 1 && p[0] <= 3)) {
		m->protocol = p[0];
		p += 1;
	} else {
		m->protocol = 0;
	}
	m->length = get64be(p);
	p += 8;
	m->chunkid = get64be(p);
	p += 8;
	m->version = get32be(p);
	p += 4;

	switch (m->protocol) {
	case 0:
		e_sz = 6;
		break;
	case 1:
		e_sz = 10;
		break;
	default:
		e_sz = 14;
		break;
	}

	while ((size_t)(end - p) >= e_sz && m->loc_count < 16) {
		m->locs[m->loc_count].ip = get32be(p);
		m->locs[m->loc_count].port = get16be(p + 4);
		if (e_sz >= 10)
			m->locs[m->loc_count].cs_ver = get32be(p + 6);
		if (e_sz >= 14)
			m->locs[m->loc_count].labelmask = get32be(p + 10);
		m->loc_count++;
		p += e_sz;
	}
	if (m->loc_count == 0)
		return MFS_ERROR_NOCHUNKSERVERS;
	return 0;
}

static int session_read_chunk_meta(struct helper_session *s,
				   uint32_t inode, uint32_t chunk_idx,
				   struct chunk_meta *m)
{
	uint8_t pkt[4 + 4 + 4 + 1];
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint32_t msgid = next_msgid(s);
	int ret;

	put32be(pkt, msgid);
	put32be(pkt + 4, inode);
	put32be(pkt + 8, chunk_idx);
	pkt[12] = 0;

	ret = master_rpc(s, CLTOMA_FUSE_READ_CHUNK, pkt, sizeof(pkt),
			 MATOCL_FUSE_READ_CHUNK, &rsp, &rsp_len);
	if (ret)
		return ret;
	ret = parse_chunk_meta_rsp(msgid, rsp, rsp_len, m);
	free(rsp);
	return ret;
}

static void read_state_invalidate(struct read_state *rs)
{
	rs->meta_valid = false;
	rs->meta_status = 0;
	rs->inode = 0;
	rs->chunk_idx = 0;
	memset(&rs->meta, 0, sizeof(rs->meta));
	read_state_drop_cs(rs);
	read_state_reset_scoreboard(rs);
	read_state_reset_prefetch(rs);
	read_state_reset_stripe(rs);
}

static void read_state_recover_after_error(struct read_state *rs)
{
	read_state_drop_cs(rs);
	read_state_reset_scoreboard(rs);
	if (rs->prefetch_len > HELPER_READ_PREFETCH_BASE)
		rs->prefetch_len = HELPER_READ_PREFETCH_BASE;
}

static void read_state_prepare_stripe(struct read_state *rs,
				      uint32_t inode, uint32_t chunk_idx)
{
	uint32_t chunks_per_band = mfs_read_chunks_per_band();
	uint64_t band = mfs_read_chunk_band(chunk_idx);
	uint32_t base = (chunk_idx / chunks_per_band) * chunks_per_band;

	if (rs->inode == inode &&
	    rs->stripe_chunk_count > 0 &&
	    rs->stripe_band == band &&
	    chunk_idx >= rs->stripe_base_chunk &&
	    chunk_idx < rs->stripe_base_chunk + rs->stripe_chunk_count)
		return;

	read_state_reset_stripe(rs);
	rs->stripe_band = band;
	rs->stripe_base_chunk = base;
	rs->stripe_chunk_count = chunks_per_band;
	vlog("read_stripe: inode=%u band=%llu base_chunk=%u chunk_count=%u focus_chunk=%u",
	     inode, (unsigned long long)band, base, chunks_per_band, chunk_idx);
}

static struct read_stripe_meta_entry *read_state_stripe_slot(struct read_state *rs,
						     uint32_t inode,
						     uint32_t chunk_idx)
{
	uint32_t slot;

	read_state_prepare_stripe(rs, inode, chunk_idx);
	if (rs->stripe_chunk_count == 0 || chunk_idx < rs->stripe_base_chunk)
		return NULL;
	slot = chunk_idx - rs->stripe_base_chunk;
	if (slot >= rs->stripe_chunk_count || slot >= HELPER_READ_STRIPE_META_MAX)
		return NULL;
	return &rs->stripe_meta[slot];
}

static void read_state_store_stripe_meta(struct read_state *rs,
					 uint32_t inode,
					 uint32_t chunk_idx,
					 int status,
					 const struct chunk_meta *m)
{
	struct read_stripe_meta_entry *ent;

	ent = read_state_stripe_slot(rs, inode, chunk_idx);
	if (!ent)
		return;
	ent->valid = true;
	ent->chunk_idx = chunk_idx;
	ent->status = status;
	if (status == 0 && m)
		ent->meta = *m;
	else
		memset(&ent->meta, 0, sizeof(ent->meta));
}

static bool read_state_lookup_stripe_meta(struct read_state *rs,
					  uint32_t inode,
					  uint32_t chunk_idx,
					  struct chunk_meta *m,
					  int *status_out)
{
	struct read_stripe_meta_entry *ent;

	ent = read_state_stripe_slot(rs, inode, chunk_idx);
	if (!ent || !ent->valid || ent->chunk_idx != chunk_idx)
		return false;
	*status_out = ent->status;
	if (ent->status == 0 && m)
		*m = ent->meta;
	return true;
}

static int read_state_fetch_one_chunk_meta(struct helper_session *s,
					   struct read_state *rs,
					   uint32_t inode,
					   uint32_t chunk_idx,
					   struct chunk_meta *m)
{
	int cached_status = 0;
	int ret;

	if (session_lookup_read_meta_cache(s, inode, chunk_idx, m, &cached_status)) {
		read_state_store_stripe_meta(rs, inode, chunk_idx, cached_status,
					     cached_status ? NULL : m);
		return cached_status;
	}

	if (!session_acquire_read_meta_fetch(s, inode, chunk_idx) &&
	    session_lookup_read_meta_cache(s, inode, chunk_idx, m, &cached_status)) {
		read_state_store_stripe_meta(rs, inode, chunk_idx, cached_status,
					     cached_status ? NULL : m);
		return cached_status;
	}

	ret = session_read_chunk_meta(s, inode, chunk_idx, m);
	if (ret) {
		if (ret == MFS_ERROR_NOCHUNKSERVERS || ret == MFS_ERROR_NOCHUNK) {
			session_store_read_meta_cache(s, inode, chunk_idx, ret, NULL);
			read_state_store_stripe_meta(rs, inode, chunk_idx, ret, NULL);
		} else {
			session_abort_read_meta_fetch(s, inode, chunk_idx);
		}
		return ret;
	}

	session_store_read_meta_cache(s, inode, chunk_idx, 0, m);
	read_state_store_stripe_meta(rs, inode, chunk_idx, 0, m);
	return 0;
}

static void read_state_fill_stripe_plan(struct helper_session *s,
					struct read_state *rs,
					uint32_t inode,
					uint32_t chunk_idx)
{
	struct chunk_meta meta;
	uint32_t first_slot;
	uint32_t max_slot;
	uint32_t i;

	read_state_prepare_stripe(rs, inode, chunk_idx);
	if (chunk_idx < rs->stripe_base_chunk)
		return;
	first_slot = chunk_idx - rs->stripe_base_chunk;
	max_slot = first_slot + HELPER_READ_STRIPE_FILL_CREDITS;
	if (max_slot > rs->stripe_chunk_count)
		max_slot = rs->stripe_chunk_count;
	if (max_slot > HELPER_READ_STRIPE_META_MAX)
		max_slot = HELPER_READ_STRIPE_META_MAX;
	for (i = first_slot; i < max_slot; i++) {
		uint32_t stripe_chunk_idx = rs->stripe_base_chunk + i;
		struct read_stripe_meta_entry *ent = &rs->stripe_meta[i];

		if (ent->valid && ent->chunk_idx == stripe_chunk_idx)
			continue;
		if (read_state_fetch_one_chunk_meta(s, rs, inode, stripe_chunk_idx, &meta) &&
		    stripe_chunk_idx == chunk_idx)
			break;
	}
}

static int read_state_get_chunk_meta(struct helper_session *s,
				     struct read_state *rs,
				     uint32_t inode, uint32_t chunk_idx,
				     struct chunk_meta *m)
{
	int cached_status = 0;
	int ret;

	if (rs->meta_valid &&
	    rs->inode == inode &&
	    rs->chunk_idx == chunk_idx) {
		if (rs->meta_status)
			return rs->meta_status;
		*m = rs->meta;
		return 0;
	}

	read_state_reset_scoreboard(rs);
	if (read_state_lookup_stripe_meta(rs, inode, chunk_idx, m, &cached_status)) {
		ret = cached_status;
	} else {
		ret = read_state_fetch_one_chunk_meta(s, rs, inode, chunk_idx, m);
		if (!ret)
			read_state_fill_stripe_plan(s, rs, inode, chunk_idx);
	}

	rs->meta_valid = true;
	rs->meta_status = ret;
	rs->inode = inode;
	rs->chunk_idx = chunk_idx;
	if (ret) {
		memset(&rs->meta, 0, sizeof(rs->meta));
		return ret;
	}
	rs->meta = *m;
	return 0;
}

static int session_write_chunk_meta(struct helper_session *s,
				    uint32_t inode, uint32_t chunk_idx,
				    uint8_t extra_chunkopflags,
				    struct chunk_meta *m)
{
	uint8_t pkt[4 + 4 + 4 + 1];
	uint32_t pkt_len = 12;
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint32_t msgid = next_msgid(s);
	uint8_t chunkopflags = CHUNKOPFLAG_CANMODTIME | extra_chunkopflags;
	int ret;

	put32be(pkt, msgid);
	put32be(pkt + 4, inode);
	put32be(pkt + 8, chunk_idx);
	if (s->master_version >= VERSION2INT(3, 0, 4)) {
		pkt[12] = chunkopflags;
		pkt_len = 13;
	}

	vlog("write_chunk_meta: inode=%u chunk=%u master_version=0x%08x pkt_len=%u flags=0x%x",
	     inode, chunk_idx, s->master_version, pkt_len, chunkopflags);

	ret = master_rpc(s, CLTOMA_FUSE_WRITE_CHUNK, pkt, pkt_len,
			 MATOCL_FUSE_WRITE_CHUNK, &rsp, &rsp_len);
	if (ret)
		return ret;
	ret = parse_chunk_meta_rsp(msgid, rsp, rsp_len, m);
	if (!ret && m->loc_count > 0) {
		vlog("write_chunk_meta: protocol=%u chunkid=%llu version=%u length=%llu loc_count=%u first_ip=0x%08x first_port=%u",
		     m->protocol, (unsigned long long)m->chunkid, m->version,
		     (unsigned long long)m->length, m->loc_count,
		     m->locs[0].ip, m->locs[0].port);
	}
	free(rsp);
	return ret;
}

static void session_invalidate_write_meta(struct helper_session *s)
{
	s->write_meta_valid = false;
	s->write_inode = 0;
	s->write_chunk_idx = 0;
	memset(&s->write_meta, 0, sizeof(s->write_meta));
	s->write_file_size = 0;
	s->write_min_chunk_off = 0;
	s->write_max_chunk_end = 0;
	s->write_cs_active = false;
	s->write_next_writeid = 1;
}

static int cs_write_finish_on_fd(int fd, const struct chunk_meta *m)
{
	uint8_t fin[12];
	int ret;

	put64be(fin, m->chunkid);
	put32be(fin + 8, m->version);
	ret = cs_send_packet(fd, CLTOCS_WRITE_FINISH, fin, sizeof(fin));
	if (ret) {
		vlog("cs_write_data: finish send failed chunkid=%llu ret=%d",
		     (unsigned long long)m->chunkid, ret);
		return ret;
	}
	vlog("cs_write_data: finish send ok chunkid=%llu",
	     (unsigned long long)m->chunkid);
	return 0;
}

static int session_finalize_active_write(struct helper_session *s)
{
	int ret;

	if (!s->write_meta_valid)
		return 0;

	ret = session_write_chunk_end(s,
				      s->write_inode,
				      s->write_chunk_idx,
				      s->write_meta.chunkid,
				      s->write_file_size,
				      s->write_min_chunk_off,
				      s->write_max_chunk_end - s->write_min_chunk_off);
	if (ret) {
		vlog("write_finalize: inode=%u chunk=%u chunkid=%llu ret=%d",
		     s->write_inode, s->write_chunk_idx,
		     (unsigned long long)s->write_meta.chunkid, ret);
		if (s->write_cs_fd >= 0) {
			close(s->write_cs_fd);
			s->write_cs_fd = -1;
		}
	}
	session_invalidate_write_meta(s);
	return ret;
}

static int session_get_write_chunk_meta(struct helper_session *s,
					uint32_t inode, uint32_t chunk_idx,
					struct chunk_meta *m)
{
	int ret;

	if (s->write_meta_valid &&
	    s->write_inode == inode &&
	    s->write_chunk_idx == chunk_idx) {
		*m = s->write_meta;
		vlog("write_chunk_meta: cache hit inode=%u chunk=%u chunkid=%llu version=%u",
		     inode, chunk_idx,
		     (unsigned long long)m->chunkid, m->version);
		return 0;
	}

	if (s->write_meta_valid) {
		ret = session_finalize_active_write(s);
		if (ret)
			return ret;
	}

	ret = session_write_chunk_meta(s, inode, chunk_idx, 0, m);
	if (ret)
		return ret;

	s->write_meta_valid = true;
	s->write_inode = inode;
	s->write_chunk_idx = chunk_idx;
	s->write_meta = *m;
	s->write_file_size = m->length;
	s->write_min_chunk_off = UINT32_MAX;
	s->write_max_chunk_end = 0;
	return 0;
}

static int session_write_chunk_end(struct helper_session *s,
			   uint32_t inode, uint32_t chunk_idx,
			   uint64_t chunkid, uint64_t file_size,
			   uint32_t chunk_off, uint32_t write_size)
{
	uint8_t pkt[4 + 8 + 4 + 4 + 8 + 1 + 4 + 4];
	uint8_t *rsp = NULL;
	uint32_t rsp_len = 0;
	uint32_t msgid = next_msgid(s);
	uint32_t pkt_len;
	uint8_t chunkopflags = 0;
	uint8_t status;
	int ret;
	uint8_t *p = pkt;

	put32be(p, msgid);
	p += 4;
	put64be(p, chunkid);
	p += 8;
	put32be(p, inode);
	p += 4;
	if (s->master_version >= VERSION2INT(3, 0, 74)) {
		put32be(p, chunk_idx);
		p += 4;
	}
	put64be(p, file_size);
	p += 8;
	if (s->master_version >= VERSION2INT(3, 0, 4)) {
		*p++ = chunkopflags;
	}
	if (s->master_version >= VERSION2INT(4, 40, 0)) {
		put32be(p, chunk_off);
		p += 4;
		put32be(p, write_size);
		p += 4;
	}
	pkt_len = (uint32_t)(p - pkt);

	vlog("write_chunk_end: inode=%u chunk=%u chunkid=%llu file_size=%llu chunk_off=%u write_size=%u master_version=0x%08x pkt_len=%u flags=0x%x",
	     inode, chunk_idx, (unsigned long long)chunkid,
	     (unsigned long long)file_size, chunk_off, write_size,
	     s->master_version, pkt_len, chunkopflags);

	ret = master_rpc(s, CLTOMA_FUSE_WRITE_CHUNK_END, pkt, pkt_len,
			 MATOCL_FUSE_WRITE_CHUNK_END, &rsp, &rsp_len);
	if (ret)
		return ret;
	ret = parse_simple_status(msgid, rsp, rsp_len, &status);
	free(rsp);
	if (ret)
		return ret;
	return status;
}

static int cs_send_packet(int fd, uint32_t type, const void *data, uint32_t len)
{
	return master_send_packet(fd, type, data, len);
}

static int cs_recv_packet(int fd, uint32_t *type, uint8_t **data, uint32_t *len)
{
	return master_recv_packet(fd, type, data, len);
}

static uint32_t crc32_sw(const uint8_t *data, size_t len)
{
	uint32_t crc = 0xffffffffU;
	size_t i;
	int j;

	for (i = 0; i < len; i++) {
		crc ^= data[i];
		for (j = 0; j < 8; j++)
			crc = (crc & 1) ? (crc >> 1) ^ CRC_POLY : (crc >> 1);
	}
	return ~crc;
}

static int cs_read_data(const struct chunk_meta *m, uint32_t chunk_off,
			const uint32_t want, uint8_t *dst, uint32_t *got)
{
	char ipbuf[64];
	struct in_addr ia;
	uint8_t cs_protover;
	int fd;
	uint8_t *pkt = NULL;
	uint32_t pkt_len;
	uint32_t total = 0;
	int ret;

	ia.s_addr = htonl(m->locs[0].ip);
	if (!inet_ntop(AF_INET, &ia, ipbuf, sizeof(ipbuf)))
		return -EINVAL;
	fd = connect_tcp(ipbuf, m->locs[0].port, HELPER_CS_TIMEOUT_MS);
	if (fd < 0)
		return fd;
	set_socket_timeouts(fd, HELPER_CS_TIMEOUT_MS);

	cs_protover = (m->loc_count > 0 &&
		       m->locs[0].cs_ver >= VERSION2INT(1, 7, 32)) ? 1 : 0;

	if (cs_protover > 0)
		pkt_len = 1 + 8 + 4 + 4 + 4;
	else
		pkt_len = 8 + 4 + 4 + 4;
	pkt = calloc(1, pkt_len);
	if (!pkt) {
		close(fd);
		return -ENOMEM;
	}

	if (cs_protover > 0) {
		pkt[0] = cs_protover;
		put64be(pkt + 1, m->chunkid);
		put32be(pkt + 9, m->version);
		put32be(pkt + 13, chunk_off);
		put32be(pkt + 17, want);
	} else {
		put64be(pkt, m->chunkid);
		put32be(pkt + 8, m->version);
		put32be(pkt + 12, chunk_off);
		put32be(pkt + 16, want);
	}

	ret = cs_send_packet(fd, CLTOCS_READ, pkt, pkt_len);
	free(pkt);
	if (ret) {
		close(fd);
		return ret;
	}

	for (;;) {
		uint32_t type;
		uint8_t *rsp = NULL;
		uint32_t rsp_len = 0;

		ret = cs_recv_packet(fd, &type, &rsp, &rsp_len);
		if (ret) {
			close(fd);
			return ret;
		}
		if (type == CSTOCL_READ_STATUS) {
			uint8_t st;
			if (rsp_len < 9) {
				free(rsp);
				close(fd);
				return -EPROTO;
			}
			st = rsp[8];
			free(rsp);
			close(fd);
			if (st != MFS_STATUS_OK)
				return st;
			*got = total;
			return 0;
		}
		if (type == CSTOCL_READ_DATA) {
			uint16_t blocknum, boff;
			uint32_t dsz;
			uint32_t roff;
			uint32_t copy_off;
			uint32_t copy_len;
			if (rsp_len < 8 + 2 + 2 + 4 + 4) {
				free(rsp);
				close(fd);
				return -EPROTO;
			}
			blocknum = get16be(rsp + 8);
			boff = get16be(rsp + 10);
			dsz = get32be(rsp + 12);
			if (rsp_len < 20 + dsz) {
				free(rsp);
				close(fd);
				return -EPROTO;
			}
			roff = (uint32_t)blocknum * MFSBLOCKSIZE + boff;
			if (roff < chunk_off) {
				copy_off = chunk_off - roff;
			} else {
				copy_off = 0;
			}
			if (copy_off < dsz && total < want) {
				copy_len = dsz - copy_off;
				if (copy_len > want - total)
					copy_len = want - total;
				memcpy(dst + total, rsp + 20 + copy_off, copy_len);
				total += copy_len;
			}
			free(rsp);
			continue;
		}
		if (type == 0 && rsp_len == 0) {
			vlog("cs_read_data: zero packet treated as connection reset");
			free(rsp);
			close(fd);
			return -ECONNRESET;
		}
		free(rsp);
		close(fd);
		return -EPROTO;
	}
}

static int read_state_get_cs_fd(struct read_state *rs,
				struct helper_session *s,
				uint32_t inode,
				const struct cs_loc *loc)
{
	char ipbuf[64];
	struct in_addr ia;
	int fd;

	if (!loc)
		return -ENOSPC;

	if (rs->cs_fd >= 0 &&
	    rs->cs_ip == loc->ip &&
	    rs->cs_port == loc->port)
		return rs->cs_fd;

	if (rs->cs_fd >= 0) {
		close(rs->cs_fd);
		rs->cs_fd = -1;
	}

	ia.s_addr = htonl(loc->ip);
	if (!inet_ntop(AF_INET, &ia, ipbuf, sizeof(ipbuf)))
		return -EINVAL;

	fd = connect_tcp(ipbuf, loc->port, helper_read_timeout_ms(s));
	if (fd < 0)
		return fd;
	set_socket_timeouts(fd, helper_read_io_timeout_ms(s));

	rs->cs_fd = fd;
	rs->cs_ip = loc->ip;
	rs->cs_port = loc->port;
	rs->preferred_ip = loc->ip;
	rs->preferred_port = loc->port;
	session_store_read_affinity(s, inode, loc->ip, loc->port);
	return fd;
}

static int cs_read_data_on_fd(int fd, const struct chunk_meta *m,
			      const struct cs_loc *loc,
			      uint32_t chunk_off, const uint32_t want,
			      uint8_t *dst, uint32_t *got)
{
	uint8_t cs_protover;
	uint8_t *pkt = NULL;
	uint32_t pkt_len;
	uint32_t total = 0;
	int ret;

	cs_protover = (loc && loc->cs_ver >= VERSION2INT(1, 7, 32)) ? 1 : 0;

	if (cs_protover > 0)
		pkt_len = 1 + 8 + 4 + 4 + 4;
	else
		pkt_len = 8 + 4 + 4 + 4;
	pkt = calloc(1, pkt_len);
	if (!pkt)
		return -ENOMEM;

	if (cs_protover > 0) {
		pkt[0] = cs_protover;
		put64be(pkt + 1, m->chunkid);
		put32be(pkt + 9, m->version);
		put32be(pkt + 13, chunk_off);
		put32be(pkt + 17, want);
	} else {
		put64be(pkt, m->chunkid);
		put32be(pkt + 8, m->version);
		put32be(pkt + 12, chunk_off);
		put32be(pkt + 16, want);
	}

	ret = cs_send_packet(fd, CLTOCS_READ, pkt, pkt_len);
	free(pkt);
	if (ret)
		return ret;

	for (;;) {
		uint32_t type;
		uint8_t *rsp = NULL;
		uint32_t rsp_len = 0;

		ret = cs_recv_packet(fd, &type, &rsp, &rsp_len);
		if (ret)
			return ret;
		if (type == CSTOCL_READ_STATUS) {
			uint8_t st;
			if (rsp_len < 9) {
				vlog("cs_read_data_on_fd: short READ_STATUS rsp_len=%u", rsp_len);
				free(rsp);
				return -EPROTO;
			}
			st = rsp[8];
			free(rsp);
			if (st != MFS_STATUS_OK) {
				vlog("cs_read_data_on_fd: READ_STATUS non-ok status=%u", st);
			}
			if (st != MFS_STATUS_OK)
				return st;
			*got = total;
			return 0;
		}
		if (type == CSTOCL_READ_DATA) {
			uint16_t blocknum, boff;
			uint32_t dsz;
			uint32_t roff;
			uint32_t copy_off;
			uint32_t copy_len;
			if (rsp_len < 8 + 2 + 2 + 4 + 4) {
				vlog("cs_read_data_on_fd: short READ_DATA header rsp_len=%u", rsp_len);
				free(rsp);
				return -EPROTO;
			}
			blocknum = get16be(rsp + 8);
			boff = get16be(rsp + 10);
			dsz = get32be(rsp + 12);
			if (rsp_len < 20 + dsz) {
				vlog("cs_read_data_on_fd: short READ_DATA body rsp_len=%u dsz=%u", rsp_len, dsz);
				free(rsp);
				return -EPROTO;
			}
			if ((uint32_t)boff + dsz > MFSBLOCKSIZE) {
				vlog("cs_read_data_on_fd: invalid READ_DATA span block=%u boff=%u dsz=%u",
				     (unsigned)blocknum, (unsigned)boff, (unsigned)dsz);
				free(rsp);
				return -EPROTO;
			}
			roff = (uint32_t)blocknum * MFSBLOCKSIZE + boff;
			if (roff < chunk_off || roff > chunk_off + want ||
			    roff + dsz > MFSCHUNKSIZE) {
				vlog("cs_read_data_on_fd: out-of-range READ_DATA roff=%u chunk_off=%u want=%u dsz=%u block=%u boff=%u",
				     (unsigned)roff, (unsigned)chunk_off, (unsigned)want,
				     (unsigned)dsz, (unsigned)blocknum, (unsigned)boff);
			}
			if (roff < chunk_off)
				copy_off = chunk_off - roff;
			else
				copy_off = 0;
			if (copy_off < dsz && total < want) {
				copy_len = dsz - copy_off;
				if (copy_len > want - total)
					copy_len = want - total;
				memcpy(dst + total, rsp + 20 + copy_off, copy_len);
				total += copy_len;
			}
			free(rsp);
			continue;
		}
		if (type == 0 && rsp_len == 0) {
			vlog("cs_read_data_on_fd: zero packet treated as connection reset");
			free(rsp);
			return -ECONNRESET;
		}
		vlog("cs_read_data_on_fd: unexpected packet type=%u rsp_len=%u", type, rsp_len);
		free(rsp);
		return -EPROTO;
	}
}

static int read_state_fetch_window(struct helper_session *s,
				   struct read_state *rs,
				   uint32_t inode,
				   uint32_t chunk_idx,
				   const struct chunk_meta *m,
				   uint32_t chunk_off,
				   uint32_t fetch_len,
				   uint8_t *fetch_buf,
				   uint32_t *got_out)
{
	uint32_t attempt_fetch_len = fetch_len;
	int ret = -ENOSPC;
	unsigned attempt;

	if (!m || m->loc_count == 0)
		return -ENOSPC;

	for (attempt = 0; attempt < 2; attempt++) {
		const struct cs_loc *preferred;
		uint32_t i;

		preferred = choose_read_replica(s, rs, inode, chunk_idx, m);
		if (preferred) {
			int csfd = read_state_get_cs_fd(rs, s, inode, preferred);

			if (csfd >= 0) {
				ret = cs_read_data_on_fd(csfd, m, preferred, chunk_off,
							 attempt_fetch_len, fetch_buf,
							 got_out);
				if (ret == 0)
					return 0;
				vlog("read_fetch: preferred replica failed inode=%u chunk=%u chunkid=%llu off=%u len=%u ip=%u port=%u ret=%d attempt=%u",
				     inode, chunk_idx,
				     (unsigned long long)m->chunkid,
				     chunk_off, attempt_fetch_len,
				     preferred->ip, preferred->port,
				     ret, attempt);
			} else {
				ret = csfd;
				vlog("read_fetch: preferred connect failed inode=%u chunk=%u chunkid=%llu off=%u len=%u ip=%u port=%u ret=%d attempt=%u",
				     inode, chunk_idx,
				     (unsigned long long)m->chunkid,
				     chunk_off, attempt_fetch_len,
				     preferred->ip, preferred->port,
				     ret, attempt);
			}
			read_state_drop_cs(rs);
		}

		for (i = 0; i < m->loc_count; i++) {
			const struct cs_loc *loc = &m->locs[i];
			int csfd;

			if (preferred && cs_loc_matches(loc, preferred))
				continue;

			csfd = read_state_get_cs_fd(rs, s, inode, loc);
			if (csfd < 0) {
				ret = csfd;
				continue;
			}

			ret = cs_read_data_on_fd(csfd, m, loc, chunk_off,
						 attempt_fetch_len, fetch_buf, got_out);
			if (ret == 0)
				return 0;
			vlog("read_fetch: alt replica failed inode=%u chunk=%u chunkid=%llu off=%u len=%u ip=%u port=%u ret=%d attempt=%u",
			     inode, chunk_idx,
			     (unsigned long long)m->chunkid,
			     chunk_off, attempt_fetch_len,
			     loc->ip, loc->port,
			     ret, attempt);
			read_state_drop_cs(rs);
		}

		if (attempt == 0 &&
		    (ret == -ECONNRESET || ret == -ETIMEDOUT || ret == -EPIPE)) {
			if (attempt_fetch_len > HELPER_READ_FOREGROUND_MAX)
				attempt_fetch_len = HELPER_READ_FOREGROUND_MAX;
			continue;
		}
		break;
	}

	return ret;
}

static int cs_recv_write_status(int fd, uint64_t want_chunkid,
				uint32_t *writeid_out)
{
	for (;;) {
		uint32_t type;
		uint8_t *rsp = NULL;
		uint32_t rsp_len = 0;
		uint64_t got_chunkid;
		uint32_t got_writeid;
		uint8_t st;
		int ret;

		ret = cs_recv_packet(fd, &type, &rsp, &rsp_len);
		if (ret)
			return ret;
		if (type != CSTOCL_WRITE_STATUS) {
			free(rsp);
			continue;
		}
		if (rsp_len < 13) {
			free(rsp);
			return -EPROTO;
		}
		got_chunkid = get64be(rsp);
		got_writeid = get32be(rsp + 8);
		st = rsp[12];
		free(rsp);
		if (got_chunkid != want_chunkid)
			return -EPROTO;
		if (writeid_out)
			*writeid_out = got_writeid;
		if (st != MFS_STATUS_OK)
			return st;
		return 0;
	}
}

static int cs_write_data(const struct chunk_meta *m, uint32_t chunk_off,
			 const uint8_t *src, uint32_t len)
{
	char ipbuf[64];
	struct in_addr ia;
	uint8_t cs_protover;
	uint8_t *pkt = NULL;
	uint32_t pkt_len;
	uint32_t sent = 0;
	uint32_t writeid = 1;
	int ret;
	int fd;

	ia.s_addr = htonl(m->locs[0].ip);
	if (!inet_ntop(AF_INET, &ia, ipbuf, sizeof(ipbuf)))
		return -EINVAL;
	fd = connect_tcp(ipbuf, m->locs[0].port, HELPER_CS_TIMEOUT_MS);
	if (fd < 0)
		return fd;
	set_socket_timeouts(fd, HELPER_CS_TIMEOUT_MS);

	cs_protover = (m->loc_count > 0 &&
		       m->locs[0].cs_ver >= VERSION2INT(1, 7, 32)) ? 1 : 0;

	if (cs_protover > 0)
		pkt_len = 1 + 8 + 4;
	else
		pkt_len = 8 + 4;
	pkt = calloc(1, pkt_len);
	if (!pkt) {
		close(fd);
		return -ENOMEM;
	}

	if (cs_protover > 0) {
		size_t off = 0;
		pkt[off++] = cs_protover;
		put64be(pkt + off, m->chunkid);
		off += 8;
		put32be(pkt + off, m->version);
	} else {
		size_t off = 0;
		put64be(pkt + off, m->chunkid);
		off += 8;
		put32be(pkt + off, m->version);
	}

	ret = cs_send_packet(fd, CLTOCS_WRITE, pkt, pkt_len);
	free(pkt);
	if (ret) {
		vlog("cs_write_data: init send failed chunkid=%llu ret=%d",
		     (unsigned long long)m->chunkid, ret);
		close(fd);
		return ret;
	}

	ret = cs_recv_write_status(fd, m->chunkid, NULL);
	if (ret) {
		vlog("cs_write_data: init status failed chunkid=%llu ret=%d",
		     (unsigned long long)m->chunkid, ret);
		close(fd);
		return ret;
	}
	vlog("cs_write_data: init status ok chunkid=%llu",
	     (unsigned long long)m->chunkid);

	while (sent < len) {
		uint32_t roff = chunk_off + sent;
		uint16_t block = roff / MFSBLOCKSIZE;
		uint16_t boff = roff % MFSBLOCKSIZE;
		uint32_t frag = MFSBLOCKSIZE - boff;
		uint32_t crc;
		uint32_t dlen;
		uint8_t *dpkt;
		uint32_t dpklen;

		if (frag > len - sent)
			frag = len - sent;
		crc = crc32_sw(src + sent, frag);
		dpklen = 8 + 4 + 2 + 2 + 4 + 4 + frag;
		dpkt = malloc(dpklen);
		if (!dpkt) {
			close(fd);
			return -ENOMEM;
		}
		put64be(dpkt, m->chunkid);
		put32be(dpkt + 8, writeid);
		put16be(dpkt + 12, block);
		put16be(dpkt + 14, boff);
		dlen = frag;
		put32be(dpkt + 16, dlen);
		put32be(dpkt + 20, crc);
		memcpy(dpkt + 24, src + sent, frag);

		ret = cs_send_packet(fd, CLTOCS_WRITE_DATA, dpkt, dpklen);
		free(dpkt);
		if (ret) {
			vlog("cs_write_data: data send failed chunkid=%llu writeid=%u ret=%d",
			     (unsigned long long)m->chunkid, writeid, ret);
			close(fd);
			return ret;
		}
		ret = cs_recv_write_status(fd, m->chunkid, NULL);
		if (ret) {
			vlog("cs_write_data: data status failed chunkid=%llu writeid=%u ret=%d",
			     (unsigned long long)m->chunkid, writeid, ret);
			close(fd);
			return ret;
		}
		vlog("cs_write_data: data status ok chunkid=%llu writeid=%u",
		     (unsigned long long)m->chunkid, writeid);
		sent += frag;
		writeid++;
	}

	{
		uint8_t fin[12];
		put64be(fin, m->chunkid);
		put32be(fin + 8, m->version);
		ret = cs_send_packet(fd, CLTOCS_WRITE_FINISH, fin, sizeof(fin));
		if (ret) {
			vlog("cs_write_data: finish send failed chunkid=%llu ret=%d",
			     (unsigned long long)m->chunkid, ret);
			close(fd);
			return ret;
		}
		vlog("cs_write_data: finish send ok chunkid=%llu",
		     (unsigned long long)m->chunkid);
	}

	vlog("cs_write_data: success chunkid=%llu total=%u",
	     (unsigned long long)m->chunkid, len);
	close(fd);
	return 0;
}

static int session_get_write_cs_fd(struct helper_session *s,
				   const struct chunk_meta *m)
{
	char ipbuf[64];
	struct in_addr ia;
	int fd;

	if (m->loc_count == 0)
		return -ENOSPC;

	if (s->write_cs_fd >= 0 &&
	    s->write_cs_ip == m->locs[0].ip &&
	    s->write_cs_port == m->locs[0].port)
		return s->write_cs_fd;

	if (s->write_cs_fd >= 0) {
		close(s->write_cs_fd);
		s->write_cs_fd = -1;
	}

	ia.s_addr = htonl(m->locs[0].ip);
	if (!inet_ntop(AF_INET, &ia, ipbuf, sizeof(ipbuf)))
		return -EINVAL;

	fd = connect_tcp(ipbuf, m->locs[0].port, HELPER_CS_TIMEOUT_MS);
	if (fd < 0)
		return fd;
	set_socket_timeouts(fd, HELPER_CS_TIMEOUT_MS);

	s->write_cs_fd = fd;
	s->write_cs_ip = m->locs[0].ip;
	s->write_cs_port = m->locs[0].port;
	return fd;
}

static int cs_write_begin_on_fd(int fd, const struct chunk_meta *m)
{
	uint8_t cs_protover;
	uint8_t *pkt = NULL;
	uint32_t pkt_len;
	int ret;

	cs_protover = (m->loc_count > 0 &&
		       m->locs[0].cs_ver >= VERSION2INT(1, 7, 32)) ? 1 : 0;

	if (cs_protover > 0)
		pkt_len = 1 + 8 + 4;
	else
		pkt_len = 8 + 4;
	pkt = calloc(1, pkt_len);
	if (!pkt)
		return -ENOMEM;

	if (cs_protover > 0) {
		size_t off = 0;
		pkt[off++] = cs_protover;
		put64be(pkt + off, m->chunkid);
		off += 8;
		put32be(pkt + off, m->version);
	} else {
		size_t off = 0;
		put64be(pkt + off, m->chunkid);
		off += 8;
		put32be(pkt + off, m->version);
	}

	ret = cs_send_packet(fd, CLTOCS_WRITE, pkt, pkt_len);
	free(pkt);
	if (ret) {
		vlog("cs_write_data: init send failed chunkid=%llu ret=%d",
		     (unsigned long long)m->chunkid, ret);
		return ret;
	}

	ret = cs_recv_write_status(fd, m->chunkid, NULL);
	if (ret) {
		vlog("cs_write_data: init status failed chunkid=%llu ret=%d",
		     (unsigned long long)m->chunkid, ret);
		return ret;
	}
	vlog("cs_write_data: init status ok chunkid=%llu",
	     (unsigned long long)m->chunkid);
	return 0;
}

static int cs_write_data_on_fd(int fd, const struct chunk_meta *m,
			       uint32_t *writeid_io,
			       uint32_t chunk_off, const uint8_t *src, uint32_t len)
{
	uint32_t sent = 0;
	uint32_t writeid;
	int ret;

	if (!writeid_io)
		return -EINVAL;
	writeid = *writeid_io;

	while (sent < len) {
		uint32_t roff = chunk_off + sent;
		uint16_t block = roff / MFSBLOCKSIZE;
		uint16_t boff = roff % MFSBLOCKSIZE;
		uint32_t frag = MFSBLOCKSIZE - boff;
		uint32_t crc;
		uint32_t dlen;
		uint8_t *dpkt;
		uint32_t dpklen;

		if (frag > len - sent)
			frag = len - sent;
		crc = crc32_sw(src + sent, frag);
		dpklen = 8 + 4 + 2 + 2 + 4 + 4 + frag;
		dpkt = malloc(dpklen);
		if (!dpkt)
			return -ENOMEM;
		put64be(dpkt, m->chunkid);
		put32be(dpkt + 8, writeid);
		put16be(dpkt + 12, block);
		put16be(dpkt + 14, boff);
		dlen = frag;
		put32be(dpkt + 16, dlen);
		put32be(dpkt + 20, crc);
		memcpy(dpkt + 24, src + sent, frag);

		ret = cs_send_packet(fd, CLTOCS_WRITE_DATA, dpkt, dpklen);
		free(dpkt);
		if (ret) {
			vlog("cs_write_data: data send failed chunkid=%llu writeid=%u ret=%d",
			     (unsigned long long)m->chunkid, writeid, ret);
			return ret;
		}
		ret = cs_recv_write_status(fd, m->chunkid, NULL);
		if (ret) {
			vlog("cs_write_data: data status failed chunkid=%llu writeid=%u ret=%d",
			     (unsigned long long)m->chunkid, writeid, ret);
			return ret;
		}
		sent += frag;
		writeid++;
	}

	*writeid_io = writeid;
	vlog("cs_write_data: success chunkid=%llu total=%u",
	     (unsigned long long)m->chunkid, len);
	return 0;
}

static int read_state_read_data(struct helper_session *s,
				struct read_state *rs,
				const struct mfs_ctrl_read_req *req,
				uint8_t **out, uint32_t *out_len)
{
	uint64_t off = req->offset;
	uint32_t max_reply = MFS_CTRL_MAX_PAYLOAD - sizeof(struct mfs_ctrl_read_rsp);
	uint32_t remain = req->size;
	uint32_t got_total = 0;
	uint8_t *payload;
	struct mfs_ctrl_read_rsp *rr;
	int ret = 0;

	if (remain > max_reply)
		remain = max_reply;

	payload = calloc(1, sizeof(*rr) + remain);
	if (!payload)
		return -ENOMEM;
	rr = (struct mfs_ctrl_read_rsp *)payload;

	while (remain > 0) {
		uint32_t chunk_idx = (uint32_t)(off / MFSCHUNKSIZE);
		uint32_t chunk_off = (uint32_t)(off % MFSCHUNKSIZE);
		uint32_t can = MFSCHUNKSIZE - chunk_off;
		uint32_t cached;
		uint32_t got = 0;
		uint32_t fetch_off;
		uint32_t fetch_len;
		uint32_t target_len;
		uint32_t active_reads;
		uint32_t min_fetch_len;
		uint8_t *fetch_buf = NULL;
		struct chunk_meta meta;
		if (can > remain)
			can = remain;

		ret = read_state_get_chunk_meta(s, rs, req->inode, chunk_idx, &meta);
		if (ret == MFS_ERROR_NOCHUNKSERVERS || ret == MFS_ERROR_NOCHUNK) {
			memset(payload + sizeof(*rr) + got_total, 0, can);
			off += can;
			got_total += can;
			remain -= can;
			continue;
		}
		if (ret)
			goto out;

		cached = read_state_copy_from_cache(rs, chunk_off, can,
						payload + sizeof(*rr) + got_total);
		if (cached > 0) {
			off += cached;
			got_total += cached;
			remain -= cached;
			if (cached == can)
				continue;
			chunk_off += cached;
			can -= cached;
		}

		if (session_try_use_prefetched_read(s, req->inode, chunk_idx,
						    chunk_off, can)) {
			cached = read_state_copy_from_cache(rs, chunk_off, can,
						payload + sizeof(*rr) + got_total);
			if (cached > 0) {
				off += cached;
				got_total += cached;
				remain -= cached;
				if (cached == can) {
					session_schedule_read_ahead(s, req->inode,
								      chunk_idx + 1);
					continue;
				}
				chunk_off += cached;
				can -= cached;
			}
		}

		fetch_off = chunk_off;
		fetch_len = can;
		target_len = rs->prefetch_len;
		min_fetch_len = HELPER_READ_FOREGROUND_MAX;
		pthread_mutex_lock(&s->read_prefetch_mu);
		active_reads = s->read_foreground_inflight;
		pthread_mutex_unlock(&s->read_prefetch_mu);
		/*
		 * READs now run on dedicated workers, so let a proven sequential
		 * stream grow its own fetch window instead of pinning every cold
		 * request to the old 4 MiB foreground cap.
		 */
		if (active_reads >= 2) {
			target_len = can;
			min_fetch_len = can;
		} else if (active_reads >= 1) {
			uint32_t shared_cap = HELPER_READ_FOREGROUND_MAX * 2U;
			if (target_len > shared_cap)
				target_len = shared_cap;
			min_fetch_len = can < (HELPER_READ_FOREGROUND_MAX * 2U) ?
				can : (HELPER_READ_FOREGROUND_MAX * 2U);
		}
		if (fetch_len < target_len)
			fetch_len = target_len;
		if (fetch_len < min_fetch_len)
			fetch_len = min_fetch_len;
		if (fetch_off + fetch_len > MFSCHUNKSIZE)
			fetch_len = MFSCHUNKSIZE - fetch_off;
		if (fetch_len < can)
			fetch_len = can;

		ret = read_state_prepare_window(rs, fetch_off, fetch_len, &fetch_buf);
		if (ret)
			goto out;

		pthread_mutex_lock(&s->read_prefetch_mu);
		s->read_foreground_inflight++;
		pthread_mutex_unlock(&s->read_prefetch_mu);
		ret = read_state_fetch_window(s, rs, req->inode, chunk_idx, &meta,
					      fetch_off, fetch_len, fetch_buf, &got);
		pthread_mutex_lock(&s->read_prefetch_mu);
		if (s->read_foreground_inflight > 0) {
			s->read_foreground_inflight--;
		}
		pthread_cond_broadcast(&s->read_prefetch_cv);
		pthread_mutex_unlock(&s->read_prefetch_mu);
		if (ret) {
			vlog("read_data: fetch failed inode=%u band=%llu chunk=%u off=%u fetch_len=%u ret=%d active_reads=%u",
			     req->inode,
			     (unsigned long long)mfs_read_band(req->offset),
			     chunk_idx, fetch_off, fetch_len, ret, active_reads);
			goto out;
		}
		if (got == 0) {
			read_state_reset_scoreboard(rs);
			break;
		}

		read_state_commit_window(rs, fetch_off, got);
		read_state_note_progress(rs, req->inode, off, got);
		if (chunk_off + can >= got)
			session_schedule_read_ahead(s, req->inode, chunk_idx + 1);

		cached = read_state_copy_from_cache(rs, chunk_off, can,
						payload + sizeof(*rr) + got_total);
		if (cached == 0)
			break;

		off += cached;
		got_total += cached;
		remain -= cached;
		if (cached < can)
			break;
	}

	rr->size = got_total;
	*out = payload;
	*out_len = sizeof(*rr) + got_total;
	return 0;

out:
	read_state_recover_after_error(rs);
	free(payload);
	return ret;
}

static int session_write_data(struct helper_session *s,
			      const struct mfs_ctrl_write_req *req,
			      const uint8_t *data,
			      uint8_t **out, uint32_t *out_len)
{
	uint64_t off = req->offset;
	uint32_t remain = req->size;
	uint32_t sent_total = 0;
	struct mfs_ctrl_write_rsp *wr;
	uint8_t *payload;
	int ret = 0;

	payload = calloc(1, sizeof(*wr));
	if (!payload)
		return -ENOMEM;
	wr = (struct mfs_ctrl_write_rsp *)payload;

	vlog("write: inode=%u offset=%llu size=%u session=%u",
	     req->inode, (unsigned long long)req->offset, req->size,
	     req->session_id);

	while (remain > 0) {
		uint32_t chunk_idx = (uint32_t)(off / MFSCHUNKSIZE);
		uint32_t chunk_off = (uint32_t)(off % MFSCHUNKSIZE);
		uint32_t can = MFSCHUNKSIZE - chunk_off;
		struct chunk_meta meta;
		int csfd;

		if (can > remain)
			can = remain;

		ret = session_get_write_chunk_meta(s, req->inode, chunk_idx, &meta);
		if (ret) {
			vlog("write: chunk_meta failed inode=%u chunk=%u ret=%d",
			     req->inode, chunk_idx, ret);
			goto out;
		}
		csfd = session_get_write_cs_fd(s, &meta);
		if (csfd < 0) {
			vlog("write: cs fd failed inode=%u chunk=%u ret=%d",
			     req->inode, chunk_idx, csfd);
			ret = csfd;
			goto out;
		}
		ret = cs_write_begin_on_fd(csfd, &meta);
		if (ret) {
			vlog("write: cs_write_begin failed inode=%u chunk=%u ret=%d",
			     req->inode, chunk_idx, ret);
			goto out;
		}
		s->write_cs_active = true;
		s->write_next_writeid = 1;
		ret = cs_write_data_on_fd(csfd, &meta, &s->write_next_writeid,
					  chunk_off, data + sent_total, can);
		if (ret) {
			vlog("write: cs_write_data failed inode=%u chunk=%u off=%u len=%u ret=%d",
			     req->inode, chunk_idx, chunk_off, can, ret);
			if (s->write_cs_fd >= 0) {
				close(s->write_cs_fd);
				s->write_cs_fd = -1;
			}
			session_invalidate_write_meta(s);
			goto out;
		}
		ret = cs_write_finish_on_fd(s->write_cs_fd, &meta);
		if (ret) {
			vlog("write: cs_write_finish failed inode=%u chunk=%u off=%u len=%u ret=%d",
			     req->inode, chunk_idx, chunk_off, can, ret);
			if (s->write_cs_fd >= 0) {
				close(s->write_cs_fd);
				s->write_cs_fd = -1;
			}
			session_invalidate_write_meta(s);
			goto out;
		}
		s->write_cs_active = false;
		s->write_next_writeid = 1;
		if (s->write_cs_fd >= 0) {
			close(s->write_cs_fd);
			s->write_cs_fd = -1;
		}
		s->write_cs_ip = 0;
		s->write_cs_port = 0;
		if (off + can > s->write_meta.length)
			s->write_meta.length = off + can;
		if (off + can > s->write_file_size)
			s->write_file_size = off + can;
		if (chunk_off < s->write_min_chunk_off)
			s->write_min_chunk_off = chunk_off;
		if (chunk_off + can > s->write_max_chunk_end)
			s->write_max_chunk_end = chunk_off + can;

		off += can;
		sent_total += can;
		remain -= can;
	}

	wr->written = sent_total;
	wr->attr_valid = 0;
	vlog("write: complete inode=%u written=%u",
	     req->inode, wr->written);

	*out = payload;
	*out_len = sizeof(*wr);
	return 0;

out:
	vlog("write: failed inode=%u offset=%llu size=%u ret=%d",
	     req->inode, (unsigned long long)req->offset, req->size, ret);
	free(payload);
	return ret;
}

static int handle_request(struct helper_session *s,
			 const struct mfs_ctrl_hdr *hdr,
			 const uint8_t *payload, uint32_t payload_len,
			 uint8_t **out_payload, uint32_t *out_len)
{
	int ret;

	*out_payload = NULL;
	*out_len = 0;

	if (s->write_meta_valid &&
	    hdr->op != MFS_CTRL_OP_WRITE &&
	    hdr->op != MFS_CTRL_OP_FSYNC) {
		ret = session_finalize_active_write(s);
		if (ret)
			return ret;
	}

	switch (hdr->op) {
	case MFS_CTRL_OP_HELLO:
		return 0;
	case MFS_CTRL_OP_REGISTER: {
		const struct mfs_ctrl_register_req *req;
		struct mfs_ctrl_register_rsp *rsp;

		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_register_req *)payload;
		rsp = calloc(1, sizeof(*rsp));
		if (!rsp)
			return -ENOMEM;
		ret = session_register(s, req, payload + sizeof(*req),
				       payload_len - sizeof(*req), rsp);
		if (ret) {
			free(rsp);
			return ret;
		}
		*out_payload = (uint8_t *)rsp;
		*out_len = sizeof(*rsp);
		return 0;
	}
	case MFS_CTRL_OP_LOOKUP: {
		const struct mfs_ctrl_lookup_req *req;
		struct mfs_ctrl_lookup_rsp *rsp;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_lookup_req *)payload;
		if (payload_len < sizeof(*req) + req->name_len)
			return -EINVAL;
		rsp = calloc(1, sizeof(*rsp));
		if (!rsp)
			return -ENOMEM;
		ret = session_lookup(s, req, payload + sizeof(*req), rsp);
		if (ret) {
			free(rsp);
			return ret;
		}
		*out_payload = (uint8_t *)rsp;
		*out_len = sizeof(*rsp);
		return 0;
	}
	case MFS_CTRL_OP_GETATTR: {
		const struct mfs_ctrl_inode_req *req;
		struct mfs_wire_attr *rsp;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_inode_req *)payload;
		vlog("CTRL_GETATTR: session=%u inode=%u uid=%u gid=%u",
		     req->session_id, req->inode, req->uid, req->gid);
		rsp = calloc(1, sizeof(*rsp));
		if (!rsp)
			return -ENOMEM;
		ret = session_getattr(s, req->inode, req->uid, req->gid, rsp);
		if (ret) {
			vlog("CTRL_GETATTR: failed ret=%d", ret);
			free(rsp);
			return ret;
		}
		*out_payload = (uint8_t *)rsp;
		*out_len = sizeof(*rsp);
		return 0;
	}
	case MFS_CTRL_OP_SETATTR: {
		const struct mfs_ctrl_setattr_req *req;
		struct mfs_wire_attr *rsp;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_setattr_req *)payload;
		rsp = calloc(1, sizeof(*rsp));
		if (!rsp)
			return -ENOMEM;
		ret = session_setattr(s, req, rsp);
		if (ret) {
			free(rsp);
			return ret;
		}
		*out_payload = (uint8_t *)rsp;
		*out_len = sizeof(*rsp);
		return 0;
	}
	case MFS_CTRL_OP_TRUNCATE: {
		const struct mfs_ctrl_truncate_req *req;
		struct mfs_wire_attr *rsp;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_truncate_req *)payload;
		rsp = calloc(1, sizeof(*rsp));
		if (!rsp)
			return -ENOMEM;
		ret = session_truncate(s, req, rsp);
		if (ret) {
			free(rsp);
			return ret;
		}
		*out_payload = (uint8_t *)rsp;
		*out_len = sizeof(*rsp);
		return 0;
	}
	case MFS_CTRL_OP_READDIR: {
		if (payload_len < sizeof(struct mfs_ctrl_readdir_req))
			return -EINVAL;
		return session_readdir(s, (const struct mfs_ctrl_readdir_req *)payload,
				      out_payload, out_len);
	}
	case MFS_CTRL_OP_CREATE: {
		const struct mfs_ctrl_create_req *req;
		struct mfs_ctrl_create_rsp *rsp;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_create_req *)payload;
		if (payload_len < sizeof(*req) + req->name_len)
			return -EINVAL;
		vlog("CTRL_CREATE: session=%u parent=%u name_len=%u mode=%o",
		     req->session_id, req->parent_inode, req->name_len,
		     req->mode & 07777);
		rsp = calloc(1, sizeof(*rsp));
		if (!rsp)
			return -ENOMEM;
		ret = session_create(s, req, payload + sizeof(*req), rsp);
		if (ret) {
			vlog("CTRL_CREATE: failed ret=%d", ret);
			free(rsp);
			return ret;
		}
		vlog("CTRL_CREATE: created inode=%u", rsp->inode);
		*out_payload = (uint8_t *)rsp;
		*out_len = sizeof(*rsp);
		return 0;
	}
	case MFS_CTRL_OP_UNLINK: {
		const struct mfs_ctrl_unlink_req *req;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_unlink_req *)payload;
		if (payload_len < sizeof(*req) + req->name_len)
			return -EINVAL;
		return session_unlink_rmdir(s, req, payload + sizeof(*req),
					   CLTOMA_FUSE_UNLINK, MATOCL_FUSE_UNLINK);
	}
	case MFS_CTRL_OP_LINK: {
		const struct mfs_ctrl_link_req *req;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_link_req *)payload;
		if (payload_len < sizeof(*req) + req->name_len)
			return -EINVAL;
		return session_link(s, req, payload + sizeof(*req));
	}
	case MFS_CTRL_OP_RENAME: {
		const struct mfs_ctrl_rename_req *req;
		const uint8_t *old_name;
		const uint8_t *new_name;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_rename_req *)payload;
		if (payload_len < sizeof(*req) + req->old_name_len + req->new_name_len)
			return -EINVAL;
		old_name = payload + sizeof(*req);
		new_name = old_name + req->old_name_len;
		return session_rename(s, req, old_name, new_name);
	}
	case MFS_CTRL_OP_MKDIR: {
		const struct mfs_ctrl_mkdir_req *req;
		struct mfs_ctrl_create_rsp *rsp;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_mkdir_req *)payload;
		if (payload_len < sizeof(*req) + req->name_len)
			return -EINVAL;
		rsp = calloc(1, sizeof(*rsp));
		if (!rsp)
			return -ENOMEM;
		ret = session_mkdir(s, req, payload + sizeof(*req), rsp);
		if (ret) {
			free(rsp);
			return ret;
		}
		*out_payload = (uint8_t *)rsp;
		*out_len = sizeof(*rsp);
		return 0;
	}
	case MFS_CTRL_OP_RMDIR: {
		struct mfs_ctrl_unlink_req req;
		if (payload_len < sizeof(struct mfs_ctrl_rmdir_req))
			return -EINVAL;
		memset(&req, 0, sizeof(req));
		{
			const struct mfs_ctrl_rmdir_req *r = (const struct mfs_ctrl_rmdir_req *)payload;
			req.session_id = r->session_id;
			req.parent_inode = r->parent_inode;
			req.uid = r->uid;
			req.gid = r->gid;
			req.name_len = r->name_len;
		}
		if (payload_len < sizeof(struct mfs_ctrl_rmdir_req) + req.name_len)
			return -EINVAL;
		return session_unlink_rmdir(s, &req,
					   payload + sizeof(struct mfs_ctrl_rmdir_req),
					   CLTOMA_FUSE_RMDIR, MATOCL_FUSE_RMDIR);
	}
	case MFS_CTRL_OP_READLINK: {
		if (payload_len < sizeof(struct mfs_ctrl_inode_req))
			return -EINVAL;
		return session_readlink(s, (const struct mfs_ctrl_inode_req *)payload,
				       out_payload, out_len);
	}
	case MFS_CTRL_OP_SYMLINK: {
		const struct mfs_ctrl_symlink_req *req;
		const uint8_t *name;
		const uint8_t *target;
		struct mfs_ctrl_create_rsp *rsp;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_symlink_req *)payload;
		if (payload_len < sizeof(*req) + req->name_len + req->target_len)
			return -EINVAL;
		name = payload + sizeof(*req);
		target = name + req->name_len;
		rsp = calloc(1, sizeof(*rsp));
		if (!rsp)
			return -ENOMEM;
		ret = session_symlink(s, req, name, target, rsp);
		if (ret) {
			free(rsp);
			return ret;
		}
		*out_payload = (uint8_t *)rsp;
		*out_len = sizeof(*rsp);
		return 0;
	}
	case MFS_CTRL_OP_GETXATTR: {
		const struct mfs_ctrl_xattr_req *req;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_xattr_req *)payload;
		if (payload_len < sizeof(*req) + req->name_len)
			return -EINVAL;
		return session_getxattr(s, req, payload + sizeof(*req),
				       out_payload, out_len);
	}
	case MFS_CTRL_OP_LISTXATTR: {
		const struct mfs_ctrl_inode_req *req;
		struct mfs_ctrl_xattr_req xq;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_inode_req *)payload;
		memset(&xq, 0, sizeof(xq));
		xq.session_id = req->session_id;
		xq.inode = req->inode;
		xq.uid = req->uid;
		xq.gid = req->gid;
		return session_getxattr(s, &xq, NULL, out_payload, out_len);
	}
	case MFS_CTRL_OP_SETXATTR: {
		const struct mfs_ctrl_xattr_req *req;
		const uint8_t *name;
		const uint8_t *value;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_xattr_req *)payload;
		if (payload_len < sizeof(*req) + req->name_len + req->value_len)
			return -EINVAL;
		name = payload + sizeof(*req);
		value = name + req->name_len;
		return session_setxattr(s, req, name, value);
	}
	case MFS_CTRL_OP_REMOVEXATTR: {
		struct mfs_ctrl_xattr_req xq;
		const struct mfs_ctrl_xattr_req *req;
		const uint8_t *name;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_xattr_req *)payload;
		if (payload_len < sizeof(*req) + req->name_len)
			return -EINVAL;
		memcpy(&xq, req, sizeof(xq));
		xq.flags = 3;
		xq.value_len = 0;
		name = payload + sizeof(*req);
		return session_setxattr(s, &xq, name, NULL);
	}
	case MFS_CTRL_OP_READ: {
		struct read_state rs;
		int ret;
		if (payload_len < sizeof(struct mfs_ctrl_read_req))
			return -EINVAL;
		read_state_init(&rs);
		ret = read_state_read_data(s, &rs,
					   (const struct mfs_ctrl_read_req *)payload,
					   out_payload, out_len);
		read_state_destroy(&rs);
		return ret;
	}
	case MFS_CTRL_OP_WRITE: {
		const struct mfs_ctrl_write_req *req;
		if (payload_len < sizeof(*req))
			return -EINVAL;
		req = (const struct mfs_ctrl_write_req *)payload;
		if (payload_len < sizeof(*req) + req->size)
			return -EINVAL;
		vlog("CTRL_WRITE: session=%u inode=%u offset=%llu size=%u",
		     req->session_id, req->inode,
		     (unsigned long long)req->offset, req->size);
		return session_write_data(s, req, payload + sizeof(*req),
					  out_payload, out_len);
	}
	case MFS_CTRL_OP_FSYNC:
		if (payload_len < sizeof(struct mfs_ctrl_fsync_req))
			return -EINVAL;
		vlog("CTRL_FSYNC");
		return session_fsync(s, (const struct mfs_ctrl_fsync_req *)payload);
	case MFS_CTRL_OP_STATFS: {
		struct mfs_ctrl_statfs_rsp *rsp;
		(void)payload;
		(void)payload_len;
		rsp = calloc(1, sizeof(*rsp));
		if (!rsp)
			return -ENOMEM;
		ret = session_statfs(s, rsp);
		if (ret) {
			free(rsp);
			return ret;
		}
		*out_payload = (uint8_t *)rsp;
		*out_len = sizeof(*rsp);
		return 0;
	}
	default:
		return -EOPNOTSUPP;
	}
}

static int send_response(int fd, const struct mfs_ctrl_hdr *req_hdr,
			 int status, const uint8_t *payload, uint32_t payload_len)
{
	struct mfs_ctrl_hdr rsp;
	uint8_t *buf;
	size_t total;
	int ret;

	if (payload_len > MFS_CTRL_MAX_PAYLOAD)
		return -EMSGSIZE;

	total = sizeof(rsp) + payload_len;
	buf = malloc(total);
	if (!buf)
		return -ENOMEM;

	rsp.magic = MFS_CTRL_MAGIC;
	rsp.version = MFS_CTRL_VERSION;
	rsp.req_id = req_hdr->req_id;
	rsp.op = req_hdr->op;
	rsp.flags = MFS_CTRL_FLAG_RESPONSE;
	rsp.status = status;
	rsp.payload_len = payload_len;
	memcpy(buf, &rsp, sizeof(rsp));
	if (payload_len)
		memcpy(buf + sizeof(rsp), payload, payload_len);

	ret = write_full(fd, buf, total);
	free(buf);
	return ret;
}

static int enqueue_read_job(struct helper_session *s,
			    const struct mfs_ctrl_hdr *hdr,
			    const uint8_t *payload,
			    uint32_t payload_len)
{
	size_t i;
	size_t free_slot = HELPER_READ_JOB_QUEUE_MAX;
	uint8_t *payload_copy = NULL;
	uint32_t preferred_worker = 0;

	if (payload_len) {
		payload_copy = malloc(payload_len);
		if (!payload_copy)
			return -ENOMEM;
		memcpy(payload_copy, payload, payload_len);
	}
	pthread_mutex_lock(&s->read_job_mu);
	if (payload_len >= sizeof(struct mfs_ctrl_read_req))
		preferred_worker = session_select_read_worker(
			s, (const struct mfs_ctrl_read_req *)payload_copy);
	for (;;) {
		for (i = 0; i < HELPER_READ_JOB_QUEUE_MAX; i++) {
			if (!s->read_jobs[i].valid) {
				free_slot = i;
				break;
			}
		}
		if (free_slot != HELPER_READ_JOB_QUEUE_MAX || s->read_worker_stop)
			break;
		pthread_cond_wait(&s->read_job_cv, &s->read_job_mu);
	}
	if (s->read_worker_stop) {
		pthread_mutex_unlock(&s->read_job_mu);
		free(payload_copy);
		return -ESHUTDOWN;
	}
	s->read_jobs[free_slot].valid = true;
	s->read_jobs[free_slot].hdr = *hdr;
	s->read_jobs[free_slot].payload = payload_copy;
	s->read_jobs[free_slot].payload_len = payload_len;
	s->read_jobs[free_slot].preferred_worker = preferred_worker;
	pthread_cond_broadcast(&s->read_job_cv);
	pthread_mutex_unlock(&s->read_job_mu);
	return 0;
}

static void *read_worker_thread_main(void *arg)
{
	struct read_worker_arg *wa = arg;
	struct helper_session *s = wa->session;
	uint32_t worker_id = wa->worker_id;
	struct read_state rs;
	uint32_t last_inode = 0;

	read_state_init(&rs);
	for (;;) {
		struct read_job job = {0};
		uint8_t *rsp_payload = NULL;
		uint32_t rsp_len = 0;
		int status;
		int ret;
		size_t i;

		pthread_mutex_lock(&s->read_job_mu);
		for (;;) {
			ssize_t fallback_idx = -1;

			if (s->read_worker_stop)
				break;
			for (i = 0; i < HELPER_READ_JOB_QUEUE_MAX; i++) {
				if (!s->read_jobs[i].valid)
					continue;
				if (s->read_jobs[i].preferred_worker != worker_id) {
					if (fallback_idx < 0 &&
					    s->read_jobs[i].preferred_worker >= HELPER_READ_WORKERS)
						fallback_idx = (ssize_t)i;
					continue;
				}
				job = s->read_jobs[i];
				memset(&s->read_jobs[i], 0, sizeof(s->read_jobs[i]));
				pthread_cond_broadcast(&s->read_job_cv);
				break;
			}
			if (!job.valid && fallback_idx >= 0) {
				i = (size_t)fallback_idx;
				if (s->read_jobs[i].valid) {
					job = s->read_jobs[i];
					memset(&s->read_jobs[i], 0, sizeof(s->read_jobs[i]));
					pthread_cond_broadcast(&s->read_job_cv);
				}
			}
			if (job.valid || s->read_worker_stop)
				break;
			pthread_cond_wait(&s->read_job_cv, &s->read_job_mu);
		}
		pthread_mutex_unlock(&s->read_job_mu);

		if (s->read_worker_stop)
			break;

		if (job.payload_len < sizeof(struct mfs_ctrl_read_req)) {
			status = -EINVAL;
		} else {
			const struct mfs_ctrl_read_req *req =
				(const struct mfs_ctrl_read_req *)job.payload;

			if (last_inode != req->inode) {
				read_state_destroy(&rs);
				read_state_init(&rs);
				last_inode = req->inode;
			}
			status = read_state_read_data(s, &rs, req,
					&rsp_payload, &rsp_len);
		}

		pthread_mutex_lock(&s->ctrl_write_mu);
		ret = send_response(s->ctrl_fd, &job.hdr, status, rsp_payload, rsp_len);
		pthread_mutex_unlock(&s->ctrl_write_mu);
		free(rsp_payload);
		free(job.payload);
		if (ret) {
			fprintf(stderr, "write ctrl failed: %s\n", strerror(-ret));
			g_stop = 1;
			break;
		}
	}

	read_state_destroy(&rs);
	return NULL;
}

static int session_start_read_workers(struct helper_session *s)
{
	size_t i;

	pthread_mutex_lock(&s->read_job_mu);
	s->read_worker_stop = false;
	memset(s->read_jobs, 0, sizeof(s->read_jobs));
	memset(s->read_stream_affinity, 0, sizeof(s->read_stream_affinity));
	s->read_stream_affinity_clock = 0;
	s->read_stream_next_worker = 0;
	pthread_mutex_unlock(&s->read_job_mu);

	for (i = 0; i < HELPER_READ_WORKERS; i++) {
		s->read_worker_args[i].session = s;
		s->read_worker_args[i].worker_id = (uint32_t)i;
		int ret = pthread_create(&s->read_workers[i], NULL,
					 read_worker_thread_main,
					 &s->read_worker_args[i]);
		if (ret) {
			pthread_mutex_lock(&s->read_job_mu);
			s->read_worker_stop = true;
			pthread_cond_broadcast(&s->read_job_cv);
			pthread_mutex_unlock(&s->read_job_mu);
			while (i-- > 0)
				pthread_join(s->read_workers[i], NULL);
			return -ret;
		}
	}
	s->read_worker_running = true;
	return 0;
}

static void session_stop_read_workers(struct helper_session *s)
{
	size_t i;

	if (!s->read_worker_running)
		return;
	pthread_mutex_lock(&s->read_job_mu);
	s->read_worker_stop = true;
	pthread_cond_broadcast(&s->read_job_cv);
	pthread_mutex_unlock(&s->read_job_mu);

	for (i = 0; i < HELPER_READ_WORKERS; i++)
		pthread_join(s->read_workers[i], NULL);
	s->read_worker_running = false;
}

static int daemonize_if_needed(void)
{
	pid_t pid;
	int fd;

	pid = fork();
	if (pid < 0)
		return -errno;
	if (pid > 0)
		exit(0);
	if (setsid() < 0)
		return -errno;

	pid = fork();
	if (pid < 0)
		return -errno;
	if (pid > 0)
		exit(0);

	umask(022);
	if (chdir("/") < 0)
		return -errno;

	fd = open("/dev/null", O_RDWR);
	if (fd >= 0) {
		dup2(fd, STDIN_FILENO);
		dup2(fd, STDOUT_FILENO);
		dup2(fd, STDERR_FILENO);
		if (fd > STDERR_FILENO)
			close(fd);
	}
	return 0;
}

int main(int argc, char **argv)
{
	const char *device = "/dev/mfs_ctrl";
	bool foreground = false;
	int opt;
	int fd;
	uint8_t *msgbuf;
	struct helper_session session;
	int ret;

	while ((opt = getopt(argc, argv, "d:fv")) != -1) {
		switch (opt) {
		case 'd':
			device = optarg;
			break;
		case 'f':
			foreground = true;
			break;
		case 'v':
			g_verbose = 1;
			break;
		default:
			fprintf(stderr, "usage: %s [-f] [-v] [-d /dev/mfs_ctrl]\n", argv[0]);
			return 2;
		}
	}

	if (!foreground) {
		ret = daemonize_if_needed();
		if (ret) {
			fprintf(stderr, "daemonize failed: %s\n", strerror(-ret));
			return 1;
		}
	}

	setvbuf(stderr, NULL, _IONBF, 0);

	{
		struct sigaction sa;

		memset(&sa, 0, sizeof(sa));
		sa.sa_handler = handle_sigint;
		sigemptyset(&sa.sa_mask);
		sigaction(SIGINT, &sa, NULL);
		sigaction(SIGTERM, &sa, NULL);
		sigaction(SIGHUP, &sa, NULL);
	}
	signal(SIGPIPE, SIG_IGN);

	fd = open(device, O_RDWR | O_CLOEXEC);
	if (fd < 0) {
		fprintf(stderr, "open %s failed: %s\n", device, strerror(errno));
		return 1;
	}

	msgbuf = malloc(HELPER_MAX_MSG);
	if (!msgbuf) {
		close(fd);
		return 1;
	}
	memset(&session, 0, sizeof(session));
	session.master_fd = -1;
	session.ctrl_fd = fd;
	pthread_mutex_init(&session.master_mu, NULL);
	pthread_cond_init(&session.master_cv, NULL);
	pthread_mutex_init(&session.read_prefetch_mu, NULL);
	pthread_cond_init(&session.read_prefetch_cv, NULL);
	pthread_mutex_init(&session.read_job_mu, NULL);
	pthread_cond_init(&session.read_job_cv, NULL);
	pthread_mutex_init(&session.getattr_cache_mu, NULL);
	pthread_cond_init(&session.getattr_cache_cv, NULL);
	pthread_mutex_init(&session.ctrl_write_mu, NULL);
	session.master_last_nop = time(NULL);
	session_start_read_prefetch_thread(&session);
	session_start_read_workers(&session);

	while (!g_stop) {
		struct pollfd pfd;
		ssize_t n;
		struct mfs_ctrl_hdr *hdr;
		uint8_t *payload;
		uint8_t *rsp_payload = NULL;
		uint32_t rsp_len = 0;
		int status;

		pfd.fd = fd;
		pfd.events = POLLIN;
		pfd.revents = 0;
		ret = poll(&pfd, 1, 250);
		if (ret < 0) {
			if (errno == EINTR)
				continue;
			fprintf(stderr, "poll ctrl failed: %s\n", strerror(errno));
			break;
		}
		if (ret == 0)
			continue;
		if (!(pfd.revents & (POLLIN | POLLERR | POLLHUP)))
			continue;

		n = read(fd, msgbuf, HELPER_MAX_MSG);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			fprintf(stderr, "read ctrl failed: %s\n", strerror(errno));
			break;
		}
		if (n == 0) {
			if (pfd.revents & (POLLHUP | POLLERR))
				break;
			continue;
		}
		if ((size_t)n < sizeof(*hdr)) {
			fprintf(stderr, "short ctrl header\n");
			continue;
		}

		hdr = (struct mfs_ctrl_hdr *)msgbuf;
		if (hdr->magic != MFS_CTRL_MAGIC || hdr->version != MFS_CTRL_VERSION ||
		    !(hdr->flags & MFS_CTRL_FLAG_REQUEST)) {
			fprintf(stderr, "bad ctrl frame\n");
			continue;
		}
		if ((size_t)n != sizeof(*hdr) + hdr->payload_len) {
			fprintf(stderr, "bad ctrl size\n");
			continue;
		}
		payload = msgbuf + sizeof(*hdr);

		if (hdr->op == MFS_CTRL_OP_READ) {
			ret = enqueue_read_job(&session, hdr, payload, hdr->payload_len);
			if (ret) {
				pthread_mutex_lock(&session.ctrl_write_mu);
				(void)send_response(fd, hdr, ret, NULL, 0);
				pthread_mutex_unlock(&session.ctrl_write_mu);
			}
		} else {
			status = handle_request(&session, hdr, payload, hdr->payload_len,
						&rsp_payload, &rsp_len);
			pthread_mutex_lock(&session.ctrl_write_mu);
			ret = send_response(fd, hdr, status, rsp_payload, rsp_len);
			pthread_mutex_unlock(&session.ctrl_write_mu);
			free(rsp_payload);
			if (ret) {
				fprintf(stderr, "write ctrl failed: %s\n", strerror(-ret));
				break;
			}
		}
	}

	session_stop_read_workers(&session);
	session_stop_read_prefetch_thread(&session);
	session_stop_master_thread(&session);
	if (session.master_fd >= 0)
		close(session.master_fd);
	free(msgbuf);
	close(fd);
	pthread_mutex_destroy(&session.master_mu);
	pthread_cond_destroy(&session.master_cv);
	pthread_mutex_destroy(&session.read_prefetch_mu);
	pthread_cond_destroy(&session.read_prefetch_cv);
	pthread_mutex_destroy(&session.read_job_mu);
	pthread_cond_destroy(&session.read_job_cv);
	pthread_mutex_destroy(&session.getattr_cache_mu);
	pthread_cond_destroy(&session.getattr_cache_cv);
	pthread_mutex_destroy(&session.ctrl_write_mu);
	return 0;
}
