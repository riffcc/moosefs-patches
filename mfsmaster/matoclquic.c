/*
 * Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
 *
 * This file is part of MooseFS.
 *
 * MooseFS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2 (only).
 *
 * MooseFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see
 * <https://www.gnu.org/licenses/>.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <errno.h>
#include <inttypes.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "MFSCommunication.h"
#include "cfg.h"
#include "chunks.h"
#include "datacachemgr.h"
#include "datapack.h"
#include "exports.h"
#include "filesystem.h"
#include "main.h"
#include "metadata.h"
#include "massert.h"
#include "mfslog.h"
#include "openfiles.h"
#include "random.h"
#include "sessions.h"
#include "sockets.h"
#include "matoclquic.h"

#ifdef HAVE_NGTCP2
#include <openssl/err.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto_ossl.h>

typedef struct ngtcp2_crypto_conn_ref ngtcp2_crypto_conn_ref;
typedef ngtcp2_conn *(*ngtcp2_crypto_get_conn)(ngtcp2_crypto_conn_ref *conn_ref);
typedef struct ngtcp2_crypto_conn_ref {
	ngtcp2_crypto_get_conn get_conn;
	void *user_data;
} ngtcp2_crypto_conn_ref;

int ngtcp2_crypto_recv_client_initial_cb(ngtcp2_conn *conn,const ngtcp2_cid *dcid,void *user_data);
int ngtcp2_crypto_recv_crypto_data_cb(ngtcp2_conn *conn,ngtcp2_encryption_level encryption_level,uint64_t offset,const uint8_t *data,size_t datalen,void *user_data);
int ngtcp2_crypto_encrypt_cb(uint8_t *dest,const ngtcp2_crypto_aead *aead,const ngtcp2_crypto_aead_ctx *aead_ctx,const uint8_t *plaintext,size_t plaintextlen,const uint8_t *nonce,size_t noncelen,const uint8_t *aad,size_t aadlen);
int ngtcp2_crypto_decrypt_cb(uint8_t *dest,const ngtcp2_crypto_aead *aead,const ngtcp2_crypto_aead_ctx *aead_ctx,const uint8_t *ciphertext,size_t ciphertextlen,const uint8_t *nonce,size_t noncelen,const uint8_t *aad,size_t aadlen);
int ngtcp2_crypto_hp_mask_cb(uint8_t *dest,const ngtcp2_crypto_cipher *hp,const ngtcp2_crypto_cipher_ctx *hp_ctx,const uint8_t *sample);
int ngtcp2_crypto_update_key_cb(ngtcp2_conn *conn,uint8_t *rx_secret,uint8_t *tx_secret,ngtcp2_crypto_aead_ctx *rx_aead_ctx,uint8_t *rx_iv,ngtcp2_crypto_aead_ctx *tx_aead_ctx,uint8_t *tx_iv,const uint8_t *current_rx_secret,const uint8_t *current_tx_secret,size_t secretlen,void *user_data);
void ngtcp2_crypto_delete_crypto_aead_ctx_cb(ngtcp2_conn *conn,ngtcp2_crypto_aead_ctx *aead_ctx,void *user_data);
void ngtcp2_crypto_delete_crypto_cipher_ctx_cb(ngtcp2_conn *conn,ngtcp2_crypto_cipher_ctx *cipher_ctx,void *user_data);
int ngtcp2_crypto_read_write_crypto_data(ngtcp2_conn *conn,ngtcp2_encryption_level encryption_level,const uint8_t *data,size_t datalen);
int ngtcp2_crypto_get_path_challenge_data_cb(ngtcp2_conn *conn,uint8_t *data,void *user_data);
int ngtcp2_crypto_version_negotiation_cb(ngtcp2_conn *conn,uint32_t version,const ngtcp2_cid *client_dcid,void *user_data);
void ngtcp2_ccerr_default(ngtcp2_ccerr *ccerr);
#endif

#define MATOCLQUIC_RECV_BUFFER_SIZE 2048
#define MATOCLQUIC_MAGIC UINT32_C(0x4D465351)
#define MATOCLQUIC_FLAG_PACKET_MODE UINT32_C(0x00000001)
#define MATOCLQUIC_FLAG_TCP_FALLBACK UINT32_C(0x00000002)
#define MATOCLQUIC_FLAG_ZERO_RTT UINT32_C(0x00000004)
#define MATOCLQUIC_FLAG_DATAGRAMS UINT32_C(0x00000008)
#define MATOCLQUIC_FLAG_TLS UINT32_C(0x00000010)

enum {
	MATOCLQUIC_BACKEND_DATAGRAM = 0,
	MATOCLQUIC_BACKEND_REALQUIC = 1
};

typedef struct matoclquic_peer {
	uint32_t peerip;
	uint16_t peerport;
	uint32_t version;
	uint8_t asize;
	uint8_t registered;
	uint8_t passwordrnd[32];
	void *sesdata;
#ifdef HAVE_NGTCP2
	uint8_t realquic;
	uint8_t realquic_handshake_complete;
	ngtcp2_conn *qconn;
	ngtcp2_crypto_ossl_ctx *qctx;
	SSL *qssl;
	ngtcp2_crypto_conn_ref qconnref;
	ngtcp2_path_storage qpath;
	ngtcp2_cid qscid;
	int64_t qstream_id;
	uint8_t qstream_open;
	uint8_t *qstream_rxbuf;
	uint64_t qstream_rxbase;
	uint32_t qstream_rxlen;
	uint32_t qstream_rxcap;
	uint8_t *qstream_txbuf;
	uint32_t qstream_txlen;
	uint32_t qstream_txcap;
	uint64_t qstream_txbase;
	uint64_t qstream_txsent;
#endif
	struct matoclquic_peer *next;
} matoclquic_peer;

static uint8_t QuicEnabled;
static uint8_t QuicBackend;
static char *QuicListenHost;
static char *QuicListenPort;
static char *QuicAlpn;
static char *QuicCertFile;
static char *QuicKeyFile;

static int lsock = -1;
static int32_t lsockpdescpos = -1;
static uint32_t listenip;
static uint16_t listenport;
static uint32_t TcpListenPort;
static uint64_t QuicDatagramsReceived;
static uint64_t QuicBytesReceived;
static matoclquic_peer *quicpeerhead;
static void matoclquic_dispatch(uint32_t peerip,uint16_t peerport,uint32_t packettype,const uint8_t *payload,uint32_t payloadleng);
#ifdef HAVE_NGTCP2
static SSL_CTX *QuicSslCtx;
#endif

static uint8_t matoclquic_backend_from_string(const char *backend) {
	if (backend!=NULL && strcmp(backend,"realquic")==0) {
		return MATOCLQUIC_BACKEND_REALQUIC;
	}
	return MATOCLQUIC_BACKEND_DATAGRAM;
}

#ifdef HAVE_NGTCP2
static ngtcp2_tstamp matoclquic_now(void) {
	return (ngtcp2_tstamp)(main_utime()*1000);
}

static int matoclquic_alpn_select_cb(SSL *ssl,const unsigned char **out,unsigned char *outlen,const unsigned char *in,unsigned int inlen,void *arg) {
	const unsigned char *ptr;
	size_t remain;
	size_t wantlen;
	(void)ssl;
	(void)arg;
	if (QuicAlpn==NULL) {
		return SSL_TLSEXT_ERR_NOACK;
	}
	wantlen = strlen(QuicAlpn);
	ptr = in;
	remain = inlen;
	while (remain>0) {
		uint8_t offeredlen;
		offeredlen = *ptr;
		ptr++;
		remain--;
		if (offeredlen>remain) {
			break;
		}
		if ((size_t)offeredlen==wantlen && memcmp(ptr,QuicAlpn,wantlen)==0) {
			*out = ptr;
			*outlen = offeredlen;
			return SSL_TLSEXT_ERR_OK;
		}
		ptr += offeredlen;
		remain -= offeredlen;
	}
	return SSL_TLSEXT_ERR_ALERT_FATAL;
}

static ngtcp2_conn* matoclquic_get_conn_ref(ngtcp2_crypto_conn_ref *conn_ref) {
	matoclquic_peer *peer = (matoclquic_peer*)conn_ref->user_data;
	return peer->qconn;
}

static void matoclquic_rand_cb(uint8_t *dest,size_t destlen,const ngtcp2_rand_ctx *rand_ctx) {
	size_t i;
	(void)rand_ctx;
	for (i=0 ; i<destlen ; i++) {
		dest[i] = rndu8();
	}
}

static int matoclquic_get_new_connection_id_cb(ngtcp2_conn *conn,ngtcp2_cid *cid,uint8_t *token,size_t cidlen,void *user_data) {
	size_t i;
	(void)conn;
	(void)user_data;
	cid->datalen = cidlen;
	for (i=0 ; i<cidlen ; i++) {
		cid->data[i] = rndu8();
	}
	for (i=0 ; i<NGTCP2_STATELESS_RESET_TOKENLEN ; i++) {
		token[i] = rndu8();
	}
	return 0;
}

static int matoclquic_handshake_completed_cb(ngtcp2_conn *conn,void *user_data) {
	matoclquic_peer *peer = (matoclquic_peer*)user_data;
	(void)conn;
	peer->realquic_handshake_complete = 1;
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,
		"main master QUIC module: QUIC handshake completed for %s",
		univallocstripport(peer->peerip,peer->peerport));
	return 0;
}

static void matoclquic_peer_free_realquic(matoclquic_peer *peer) {
	if (peer->qssl!=NULL) {
		SSL_set_app_data(peer->qssl,NULL);
		SSL_free(peer->qssl);
		peer->qssl = NULL;
	}
	if (peer->qctx!=NULL) {
		ngtcp2_crypto_ossl_ctx_del(peer->qctx);
		peer->qctx = NULL;
	}
	if (peer->qconn!=NULL) {
		ngtcp2_conn_del(peer->qconn);
		peer->qconn = NULL;
	}
	peer->realquic = 0;
	peer->realquic_handshake_complete = 0;
	memset(&peer->qpath,0,sizeof(peer->qpath));
	memset(&peer->qscid,0,sizeof(peer->qscid));
	peer->qstream_id = -1;
	peer->qstream_open = 0;
	if (peer->qstream_rxbuf!=NULL) {
		free(peer->qstream_rxbuf);
		peer->qstream_rxbuf = NULL;
	}
	peer->qstream_rxbase = 0;
	peer->qstream_rxlen = 0;
	peer->qstream_rxcap = 0;
	if (peer->qstream_txbuf!=NULL) {
		free(peer->qstream_txbuf);
		peer->qstream_txbuf = NULL;
	}
	peer->qstream_txlen = 0;
	peer->qstream_txcap = 0;
	peer->qstream_txbase = 0;
	peer->qstream_txsent = 0;
}

static void matoclquic_fill_sockaddrs(struct sockaddr_in *localsa,struct sockaddr_in *remotesa,uint32_t peerip,uint16_t peerport) {
	memset(localsa,0,sizeof(struct sockaddr_in));
	localsa->sin_family = AF_INET;
	localsa->sin_port = htons(listenport);
	localsa->sin_addr.s_addr = htonl(listenip);

	memset(remotesa,0,sizeof(struct sockaddr_in));
	remotesa->sin_family = AF_INET;
	remotesa->sin_port = htons(peerport);
	remotesa->sin_addr.s_addr = htonl(peerip);
}

static int matoclquic_stream_reserve(uint8_t **buf,uint32_t *cap,uint32_t need) {
	uint8_t *nbuf;
	uint32_t ncap;
	if (*cap>=need) {
		return 0;
	}
	ncap = (*cap==0)?1024:*cap;
	while (ncap<need) {
		ncap*=2;
	}
	nbuf = realloc(*buf,ncap);
	if (nbuf==NULL) {
		return -1;
	}
	*buf = nbuf;
	*cap = ncap;
	return 0;
}

static int matoclquic_stream_queue_packet(matoclquic_peer *peer,uint32_t packettype,const uint8_t *payload,uint32_t payloadleng) {
	uint8_t *ptr;
	uint32_t need;
	if (peer==NULL || peer->realquic==0 || peer->realquic_handshake_complete==0 || peer->qstream_open==0 || peer->qstream_id<0) {
		return -1;
	}
	need = peer->qstream_txlen + payloadleng + 8;
	if (matoclquic_stream_reserve(&peer->qstream_txbuf,&peer->qstream_txcap,need)<0) {
		return -1;
	}
	ptr = peer->qstream_txbuf + peer->qstream_txlen;
	put32bit(&ptr,packettype);
	put32bit(&ptr,payloadleng);
	if (payloadleng>0 && payload!=NULL) {
		memcpy(ptr,payload,payloadleng);
	}
	peer->qstream_txlen += payloadleng + 8;
	return 0;
}

static int matoclquic_recv_stream_data_cb(ngtcp2_conn *conn,uint32_t flags,int64_t stream_id,uint64_t offset,const uint8_t *data,size_t datalen,void *user_data,void *stream_user_data) {
	matoclquic_peer *peer = (matoclquic_peer*)user_data;
	size_t skip;
	const uint8_t *ptr;
	uint32_t packettype;
	uint32_t payloadleng;
	(void)conn;
	(void)stream_user_data;
	(void)flags;
	if (peer->qstream_open==0) {
		peer->qstream_open = 1;
		peer->qstream_id = stream_id;
	} else if (peer->qstream_id!=stream_id) {
		return NGTCP2_ERR_CALLBACK_FAILURE;
	}
	if (offset < peer->qstream_rxbase) {
		skip = (size_t)(peer->qstream_rxbase - offset);
		if (skip >= datalen) {
			return 0;
		}
		offset = peer->qstream_rxbase;
		data += skip;
		datalen -= skip;
	}
	if (offset!=(peer->qstream_rxbase + (uint64_t)peer->qstream_rxlen)) {
		return NGTCP2_ERR_CALLBACK_FAILURE;
	}
	if (datalen>0) {
		if (matoclquic_stream_reserve(&peer->qstream_rxbuf,&peer->qstream_rxcap,peer->qstream_rxlen+(uint32_t)datalen)<0) {
			return NGTCP2_ERR_CALLBACK_FAILURE;
		}
		memcpy(peer->qstream_rxbuf+peer->qstream_rxlen,data,datalen);
		peer->qstream_rxlen += (uint32_t)datalen;
	}
	while (peer->qstream_rxlen>=8) {
		ptr = peer->qstream_rxbuf;
		packettype = get32bit(&ptr);
		payloadleng = get32bit(&ptr);
		if (peer->qstream_rxlen < payloadleng + 8) {
			break;
		}
		matoclquic_dispatch(peer->peerip,peer->peerport,packettype,ptr,payloadleng);
		if (peer->qstream_rxlen > payloadleng + 8) {
			memmove(peer->qstream_rxbuf,peer->qstream_rxbuf+payloadleng+8,peer->qstream_rxlen-(payloadleng+8));
		}
		peer->qstream_rxbase += payloadleng + 8;
		peer->qstream_rxlen -= payloadleng + 8;
	}
	return 0;
}

static int matoclquic_acked_stream_data_offset_cb(ngtcp2_conn *conn,int64_t stream_id,uint64_t offset,uint64_t datalen,void *user_data,void *stream_user_data) {
	matoclquic_peer *peer = (matoclquic_peer*)user_data;
	uint64_t rel;
	(void)conn;
	(void)stream_user_data;
	if (peer->qstream_open==0 || peer->qstream_id!=stream_id) {
		return 0;
	}
	if (offset!=peer->qstream_txbase || datalen>(uint64_t)peer->qstream_txlen) {
		return 0;
	}
	rel = datalen;
	if (peer->qstream_txlen > (uint32_t)rel) {
		memmove(peer->qstream_txbuf,peer->qstream_txbuf+rel,peer->qstream_txlen-(uint32_t)rel);
	}
	peer->qstream_txlen -= (uint32_t)rel;
	peer->qstream_txbase += rel;
	if (peer->qstream_txsent >= rel) {
		peer->qstream_txsent -= rel;
	} else {
		peer->qstream_txsent = 0;
	}
	return 0;
}

static int matoclquic_stream_open_cb(ngtcp2_conn *conn,int64_t stream_id,void *user_data) {
	matoclquic_peer *peer = (matoclquic_peer*)user_data;
	(void)conn;
	if (peer->qstream_open==0) {
		peer->qstream_open = 1;
		peer->qstream_id = stream_id;
	}
	return 0;
}

static int matoclquic_stream_close_cb(ngtcp2_conn *conn,uint32_t flags,int64_t stream_id,uint64_t app_error_code,void *user_data,void *stream_user_data) {
	matoclquic_peer *peer = (matoclquic_peer*)user_data;
	(void)conn;
	(void)flags;
	(void)app_error_code;
	(void)stream_user_data;
	if (peer->qstream_id==stream_id) {
		peer->qstream_open = 0;
		peer->qstream_id = -1;
		peer->qstream_rxbase = 0;
		peer->qstream_rxlen = 0;
		peer->qstream_txlen = 0;
		peer->qstream_txbase = 0;
		peer->qstream_txsent = 0;
	}
	return 0;
}

static int matoclquic_peer_init_realquic(matoclquic_peer *peer,const uint8_t *buff,uint32_t leng) {
	ngtcp2_callbacks callbacks;
	ngtcp2_settings settings;
	ngtcp2_transport_params params;
	ngtcp2_version_cid versioncid;
	struct sockaddr_in localaddr;
	struct sockaddr_in remoteaddr;
	ngtcp2_cid clientscid;
	ngtcp2_cid originaldcid;
	int rv;
	uint32_t i;

	if (peer->realquic) {
		return 0;
	}
	if (ngtcp2_pkt_decode_version_cid(&versioncid,buff,leng,0)<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: failed to decode version/cid from %s",
			univallocstripport(peer->peerip,peer->peerport));
		return -1;
	}
	if (versioncid.scidlen>NGTCP2_MAX_CIDLEN || versioncid.dcidlen==0 || versioncid.dcidlen>NGTCP2_MAX_CIDLEN) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: invalid cid lengths from %s (scid=%zu dcid=%zu)",
			univallocstripport(peer->peerip,peer->peerport),versioncid.scidlen,versioncid.dcidlen);
		return -1;
	}

	memset(&callbacks,0,sizeof(callbacks));
	callbacks.recv_client_initial = ngtcp2_crypto_recv_client_initial_cb;
	callbacks.recv_crypto_data = ngtcp2_crypto_recv_crypto_data_cb;
	callbacks.handshake_completed = matoclquic_handshake_completed_cb;
	callbacks.encrypt = ngtcp2_crypto_encrypt_cb;
	callbacks.decrypt = ngtcp2_crypto_decrypt_cb;
	callbacks.hp_mask = ngtcp2_crypto_hp_mask_cb;
	callbacks.rand = matoclquic_rand_cb;
	callbacks.get_new_connection_id = matoclquic_get_new_connection_id_cb;
	callbacks.update_key = ngtcp2_crypto_update_key_cb;
	callbacks.delete_crypto_aead_ctx = ngtcp2_crypto_delete_crypto_aead_ctx_cb;
	callbacks.delete_crypto_cipher_ctx = ngtcp2_crypto_delete_crypto_cipher_ctx_cb;
	callbacks.get_path_challenge_data = ngtcp2_crypto_get_path_challenge_data_cb;
	callbacks.version_negotiation = ngtcp2_crypto_version_negotiation_cb;
	callbacks.recv_stream_data = matoclquic_recv_stream_data_cb;
	callbacks.acked_stream_data_offset = matoclquic_acked_stream_data_offset_cb;
	callbacks.stream_open = matoclquic_stream_open_cb;
	callbacks.stream_close = matoclquic_stream_close_cb;

	memset(&clientscid,0,sizeof(clientscid));
	clientscid.datalen = versioncid.scidlen;
	memcpy(clientscid.data,versioncid.scid,versioncid.scidlen);

	memset(&originaldcid,0,sizeof(originaldcid));
	originaldcid.datalen = versioncid.dcidlen;
	memcpy(originaldcid.data,versioncid.dcid,versioncid.dcidlen);

	memset(&peer->qscid,0,sizeof(peer->qscid));
	peer->qscid.datalen = 16;
	for (i=0 ; i<peer->qscid.datalen ; i++) {
		peer->qscid.data[i] = rndu8();
	}

	ngtcp2_settings_default(&settings);
	settings.initial_ts = matoclquic_now();

	ngtcp2_transport_params_default(&params);
	params.initial_max_stream_data_bidi_local = 256*1024;
	params.initial_max_stream_data_bidi_remote = 256*1024;
	params.initial_max_stream_data_uni = 256*1024;
	params.initial_max_data = 1024*1024;
	params.initial_max_streams_bidi = 8;
	params.initial_max_streams_uni = 8;
	params.max_idle_timeout = 30*NGTCP2_SECONDS;
	params.active_connection_id_limit = 4;
	params.max_datagram_frame_size = MATOCLQUIC_RECV_BUFFER_SIZE;
	params.original_dcid = originaldcid;
	params.original_dcid_present = 1;
	for (i=0 ; i<NGTCP2_STATELESS_RESET_TOKENLEN ; i++) {
		params.stateless_reset_token[i] = rndu8();
	}
	params.stateless_reset_token_present = 1;

	matoclquic_fill_sockaddrs(&localaddr,&remoteaddr,peer->peerip,peer->peerport);
	ngtcp2_path_storage_init(&peer->qpath,(const ngtcp2_sockaddr*)&localaddr,sizeof(localaddr),(const ngtcp2_sockaddr*)&remoteaddr,sizeof(remoteaddr),NULL);

	rv = ngtcp2_conn_server_new(&peer->qconn,&clientscid,&peer->qscid,&peer->qpath.path,versioncid.version,&callbacks,&settings,&params,NULL,peer);
	if (rv!=0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: ngtcp2_conn_server_new failed for %s: %s",
			univallocstripport(peer->peerip,peer->peerport),ngtcp2_strerror(rv));
		matoclquic_peer_free_realquic(peer);
		return -1;
	}

	peer->qssl = SSL_new(QuicSslCtx);
	if (peer->qssl==NULL) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: SSL_new failed for %s",
			univallocstripport(peer->peerip,peer->peerport));
		matoclquic_peer_free_realquic(peer);
		return -1;
	}
	if (ngtcp2_crypto_ossl_ctx_new(&peer->qctx,peer->qssl)!=0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: ngtcp2_crypto_ossl_ctx_new failed for %s",
			univallocstripport(peer->peerip,peer->peerport));
		matoclquic_peer_free_realquic(peer);
		return -1;
	}
	peer->qconnref.get_conn = matoclquic_get_conn_ref;
	peer->qconnref.user_data = peer;
	if (ngtcp2_crypto_ossl_configure_server_session(peer->qssl)!=0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: ngtcp2_crypto_ossl_configure_server_session failed for %s",
			univallocstripport(peer->peerip,peer->peerport));
		matoclquic_peer_free_realquic(peer);
		return -1;
	}
	SSL_set_app_data(peer->qssl,&peer->qconnref);
	SSL_set_accept_state(peer->qssl);
	ngtcp2_conn_set_tls_native_handle(peer->qconn,peer->qctx);
	peer->realquic = 1;
	peer->qstream_id = -1;

	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,
		"main master QUIC module: initialized ngtcp2 connection for %s (version: 0x%08"PRIX32")",
		univallocstripport(peer->peerip,peer->peerport),versioncid.version);
	return 0;
}

static void matoclquic_flush_realquic(matoclquic_peer *peer) {
	uint8_t out[1500];
	ngtcp2_pkt_info pi;
	ngtcp2_ssize nwritten;
	uint32_t sentpackets = 0;
	ngtcp2_ssize ndatalen;
	ngtcp2_vec vec;

	if (peer->qconn==NULL) {
		return;
	}
	memset(&pi,0,sizeof(pi));
	while (peer->qstream_open && peer->qstream_id>=0 && peer->qstream_txsent < peer->qstream_txlen) {
		vec.base = peer->qstream_txbuf + peer->qstream_txsent;
		vec.len = peer->qstream_txlen - peer->qstream_txsent;
		nwritten = ngtcp2_conn_writev_stream(peer->qconn,&peer->qpath.path,&pi,out,sizeof(out),&ndatalen,NGTCP2_WRITE_STREAM_FLAG_MORE,peer->qstream_id,&vec,1,matoclquic_now());
		if (nwritten<0) {
			if (nwritten==NGTCP2_ERR_WRITE_MORE && ndatalen>0) {
				peer->qstream_txsent += (uint64_t)ndatalen;
				continue;
			}
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
				"main master QUIC module: ngtcp2_conn_writev_stream failed for %s: %s",
				univallocstripport(peer->peerip,peer->peerport),ngtcp2_strerror((int)nwritten));
			break;
		}
		if (ndatalen>0) {
			peer->qstream_txsent += (uint64_t)ndatalen;
		}
		if (nwritten==0) {
			break;
		}
		if (udpwrite(lsock,peer->peerip,peer->peerport,out,(uint16_t)nwritten)<0) {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_NOTICE,
				"main master QUIC module: failed to send realquic stream packet to %s",
				univallocstripport(peer->peerip,peer->peerport));
			break;
		}
		sentpackets++;
	}
	for (;;) {
		nwritten = ngtcp2_conn_write_pkt(peer->qconn,&peer->qpath.path,&pi,out,sizeof(out),matoclquic_now());
		if (nwritten==0) {
			break;
		}
		if (nwritten<0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
				"main master QUIC module: ngtcp2_conn_write_pkt failed for %s: %s",
				univallocstripport(peer->peerip,peer->peerport),ngtcp2_strerror((int)nwritten));
			break;
		}
		if (udpwrite(lsock,peer->peerip,peer->peerport,out,(uint32_t)nwritten)<0) {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_NOTICE,
				"main master QUIC module: failed to send realquic packet to %s",
				univallocstripport(peer->peerip,peer->peerport));
			break;
		}
		sentpackets++;
	}
	if (sentpackets>0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,
			"main master QUIC module: flushed %"PRIu32" QUIC packet(s) to %s",
			sentpackets,univallocstripport(peer->peerip,peer->peerport));
	}
}

static void matoclquic_handle_realquic_datagram(matoclquic_peer *peer,const uint8_t *buff,uint32_t leng) {
	ngtcp2_pkt_hd hd;
	ngtcp2_pkt_info pi;
	int rv;

	if (peer->realquic==0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,
			"main master QUIC module: first realquic datagram from %s (%"PRIu32" bytes)",
			univallocstripport(peer->peerip,peer->peerport),leng);
		if (ngtcp2_accept(&hd,buff,leng)!=0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,
				"main master QUIC module: ignoring non-initial realquic datagram from %s",
				univallocstripport(peer->peerip,peer->peerport));
			return;
		}
		if (matoclquic_peer_init_realquic(peer,buff,leng)<0) {
			return;
		}
	}
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,
		"main master QUIC module: processing realquic datagram from %s (%"PRIu32" bytes)",
		univallocstripport(peer->peerip,peer->peerport),leng);
	memset(&pi,0,sizeof(pi));
	rv = ngtcp2_conn_read_pkt(peer->qconn,&peer->qpath.path,&pi,buff,leng,matoclquic_now());
	if (rv!=0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: ngtcp2_conn_read_pkt failed for %s: %s",
			univallocstripport(peer->peerip,peer->peerport),ngtcp2_strerror(rv));
		return;
	}
	matoclquic_flush_realquic(peer);
}
#endif

static const char* matoclquic_backend_name(uint8_t backend) {
	if (backend==MATOCLQUIC_BACKEND_REALQUIC) {
		return "realquic";
	}
	return "datagram";
}

static void matoclquic_close_runtime(void) {
#ifdef HAVE_NGTCP2
	if (QuicSslCtx!=NULL) {
		SSL_CTX_free(QuicSslCtx);
		QuicSslCtx = NULL;
	}
#endif
}

static int matoclquic_init_realquic_runtime(void) {
#ifdef HAVE_NGTCP2
	if (QuicCertFile==NULL || QuicKeyFile==NULL || QuicCertFile[0]==0 || QuicKeyFile[0]==0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: backend '%s' needs MATOCL_QUIC_CERT_FILE and MATOCL_QUIC_KEY_FILE",
			matoclquic_backend_name(QuicBackend));
		return -1;
	}
	if (ngtcp2_crypto_ossl_init()!=0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: ngtcp2 OpenSSL helper init failed");
		return -1;
	}
	QuicSslCtx = SSL_CTX_new(TLS_method());
	if (QuicSslCtx==NULL) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: SSL_CTX_new failed");
		return -1;
	}
	SSL_CTX_set_min_proto_version(QuicSslCtx,TLS1_3_VERSION);
	SSL_CTX_set_alpn_select_cb(QuicSslCtx,matoclquic_alpn_select_cb,NULL);
	if (SSL_CTX_use_certificate_file(QuicSslCtx,QuicCertFile,SSL_FILETYPE_PEM)!=1) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: can't load cert file %s",QuicCertFile);
		matoclquic_close_runtime();
		return -1;
	}
	if (SSL_CTX_use_PrivateKey_file(QuicSslCtx,QuicKeyFile,SSL_FILETYPE_PEM)!=1) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: can't load key file %s",QuicKeyFile);
		matoclquic_close_runtime();
		return -1;
	}
	if (SSL_CTX_check_private_key(QuicSslCtx)!=1) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: cert/key mismatch");
		matoclquic_close_runtime();
		return -1;
	}
	return 0;
#else
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
		"main master QUIC module: backend '%s' requested but libngtcp2 support is not compiled in",
		matoclquic_backend_name(QuicBackend));
	return -1;
#endif
}

static uint8_t matoclquic_attr_size_for_version(uint32_t version) {
	return (version>=VERSION2INT(3,0,93) && version!=VERSION2INT(4,0,0) && version!=VERSION2INT(4,0,1)) ? ATTR_RECORD_SIZE : 35;
}

static matoclquic_peer* matoclquic_find_peer(uint32_t peerip,uint16_t peerport) {
	matoclquic_peer *peer;
	for (peer=quicpeerhead ; peer ; peer=peer->next) {
		if (peer->peerip==peerip && peer->peerport==peerport) {
			return peer;
		}
	}
	return NULL;
}

static matoclquic_peer* matoclquic_get_peer(uint32_t peerip,uint16_t peerport) {
	matoclquic_peer *peer;
	peer = matoclquic_find_peer(peerip,peerport);
	if (peer!=NULL) {
		return peer;
	}
	peer = malloc(sizeof(matoclquic_peer));
	passert(peer);
	peer->peerip = peerip;
	peer->peerport = peerport;
	peer->version = 0;
	peer->asize = 35;
	peer->registered = 0;
	memset(peer->passwordrnd,0,32);
	peer->sesdata = NULL;
#ifdef HAVE_NGTCP2
	peer->realquic = 0;
	peer->realquic_handshake_complete = 0;
	peer->qconn = NULL;
	peer->qctx = NULL;
	peer->qssl = NULL;
	memset(&peer->qconnref,0,sizeof(peer->qconnref));
	memset(&peer->qpath,0,sizeof(peer->qpath));
	memset(&peer->qscid,0,sizeof(peer->qscid));
	peer->qstream_id = -1;
	peer->qstream_open = 0;
	peer->qstream_rxbuf = NULL;
	peer->qstream_rxlen = 0;
	peer->qstream_rxcap = 0;
	peer->qstream_txbuf = NULL;
	peer->qstream_txlen = 0;
	peer->qstream_txcap = 0;
	peer->qstream_txbase = 0;
	peer->qstream_txsent = 0;
#endif
	peer->next = quicpeerhead;
	quicpeerhead = peer;
	return peer;
}

static void matoclquic_send_raw(uint32_t peerip,uint16_t peerport,uint32_t packettype,const uint8_t *payload,uint32_t payloadleng) {
	uint8_t *packet;
	uint8_t *ptr;
	matoclquic_peer *peer;

	peer = matoclquic_find_peer(peerip,peerport);
#ifdef HAVE_NGTCP2
	if (peer!=NULL && matoclquic_stream_queue_packet(peer,packettype,payload,payloadleng)==0) {
		matoclquic_flush_realquic(peer);
		return;
	}
#endif

	packet = malloc(payloadleng+8);
	passert(packet);
	ptr = packet;
	put32bit(&ptr,packettype);
	put32bit(&ptr,payloadleng);
	if (payloadleng>0 && payload!=NULL) {
		memcpy(ptr,payload,payloadleng);
	}
	if (udpwrite(lsock,peerip,peerport,packet,payloadleng+8)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_NOTICE,"main master QUIC module: failed to send packet %"PRIu32,packettype);
	}
	free(packet);
}

static void matoclquic_send_register_status(uint32_t peerip,uint16_t peerport,uint8_t status) {
	uint8_t payload[1];
	payload[0] = status;
	matoclquic_send_raw(peerip,peerport,MATOCL_FUSE_REGISTER,payload,1);
}

static void matoclquic_send_msg_status(uint32_t peerip,uint16_t peerport,uint32_t packettype,uint32_t msgid,uint8_t status) {
	uint8_t payload[5];
	uint8_t *ptr;
	ptr = payload;
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	matoclquic_send_raw(peerip,peerport,packettype,payload,5);
}

static void matoclquic_send_hello(uint32_t peerip,uint16_t peerport,uint8_t status,uint32_t flags,const uint8_t *alpn,uint8_t alpnleng) {
	uint8_t payload[MATOCLQUIC_RECV_BUFFER_SIZE];
	uint8_t *ptr;
	ptr = payload;
	put8bit(&ptr,status);
	put32bit(&ptr,VERSION2INT(VERSMAJ,VERSMID,VERSMIN));
	put32bit(&ptr,flags);
	put16bit(&ptr,(uint16_t)TcpListenPort);
	put16bit(&ptr,listenport);
	put16bit(&ptr,MATOCLQUIC_RECV_BUFFER_SIZE);
	put8bit(&ptr,alpnleng);
	if (alpnleng>0) {
		memcpy(ptr,alpn,alpnleng);
		ptr += alpnleng;
	}
	matoclquic_send_raw(peerip,peerport,MATOCL_QUIC_HELLO,payload,(uint32_t)(ptr-payload));
}

static void matoclquic_send_register_ok(matoclquic_peer *peer,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint16_t sclassgroups,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables) {
	uint8_t payload[64];
	uint8_t *ptr;
	uint32_t sessionid;
	uint32_t payloadleng;

	sessionid = sessions_get_id(peer->sesdata);
	ptr = payload;
	put16bit(&ptr,VERSMAJ);
	put8bit(&ptr,VERSMID);
	put8bit(&ptr,VERSMIN);
	put32bit(&ptr,sessionid);
	put64bit(&ptr,meta_get_id());
	put8bit(&ptr,sesflags);
	put16bit(&ptr,umaskval);
	put32bit(&ptr,rootuid);
	put32bit(&ptr,rootgid);
	put32bit(&ptr,mapalluid);
	put32bit(&ptr,mapallgid);
	put16bit(&ptr,sclassgroups);
	put32bit(&ptr,mintrashretention);
	put32bit(&ptr,maxtrashretention);
	put32bit(&ptr,disables);
	put64bit(&ptr,0);
	payloadleng = (uint32_t)(ptr-payload);
	matoclquic_send_raw(peer->peerip,peer->peerport,MATOCL_FUSE_REGISTER,payload,payloadleng);
}

static void matoclquic_handle_register_newsession(matoclquic_peer *peer,const uint8_t *data,uint32_t leng) {
	const uint8_t *rptr;
	uint32_t version;
	uint32_t ileng;
	uint32_t pleng;
	const uint8_t *info;
	const uint8_t *path;
	const uint8_t *passcode;
	uint8_t status;
	uint8_t sesflags;
	uint16_t umaskval;
	uint16_t sclassgroups;
	uint32_t mintrashretention,maxtrashretention;
	uint32_t disables;
	uint32_t rootuid,rootgid;
	uint32_t mapalluid,mapallgid;
	uint32_t rootinode;
	uint32_t sessionid;

	if (leng<77) {
		matoclquic_send_register_status(peer->peerip,peer->peerport,MFS_ERROR_EINVAL);
		return;
	}
	rptr = data + 65;
	version = get32bit(&rptr);
	if (leng<77) {
		matoclquic_send_register_status(peer->peerip,peer->peerport,MFS_ERROR_EINVAL);
		return;
	}
	ileng = get32bit(&rptr);
	if (leng<77+ileng) {
		matoclquic_send_register_status(peer->peerip,peer->peerport,MFS_ERROR_EINVAL);
		return;
	}
	info = rptr;
	rptr += ileng;
	pleng = get32bit(&rptr);
	if (!(leng==77+ileng+pleng || leng==77+16+ileng+pleng || leng==77+4+ileng+pleng || leng==77+4+16+ileng+pleng || leng==77+12+ileng+pleng || leng==77+12+16+ileng+pleng)) {
		matoclquic_send_register_status(peer->peerip,peer->peerport,MFS_ERROR_EINVAL);
		return;
	}
	path = rptr;
	rptr += pleng;
	if (pleng==0 || path[pleng-1]!=0) {
		matoclquic_send_register_status(peer->peerip,peer->peerport,MFS_ERROR_EINVAL);
		return;
	}
	if (leng==77+4+ileng+pleng || leng==77+4+16+ileng+pleng) {
		sessionid = get32bit(&rptr);
	} else if (leng==77+12+ileng+pleng || leng==77+12+16+ileng+pleng) {
		sessionid = get32bit(&rptr);
		if (get64bit(&rptr)!=meta_get_id()) {
			sessionid = 0;
		}
	} else {
		sessionid = 0;
	}
	if (leng>=77+16+ileng+pleng) {
		passcode = rptr;
		status = exports_check(peer->peerip,version,path,peer->passwordrnd,passcode,&sesflags,&umaskval,&rootuid,&rootgid,&mapalluid,&mapallgid,&sclassgroups,&mintrashretention,&maxtrashretention,&disables);
	} else {
		passcode = NULL;
		status = exports_check(peer->peerip,version,path,NULL,NULL,&sesflags,&umaskval,&rootuid,&rootgid,&mapalluid,&mapallgid,&sclassgroups,&mintrashretention,&maxtrashretention,&disables);
	}
	if (status!=MFS_STATUS_OK) {
		matoclquic_send_register_status(peer->peerip,peer->peerport,status);
		return;
	}
	status = fs_getrootinode(&rootinode,path);
	if (status!=MFS_STATUS_OK) {
		matoclquic_send_register_status(peer->peerip,peer->peerport,status);
		return;
	}
	if (peer->sesdata!=NULL) {
		sessions_disconnection(peer->sesdata);
		peer->sesdata = NULL;
	}
	if (sessionid!=0) {
		peer->sesdata = sessions_find_session(sessionid);
		if (peer->sesdata!=NULL) {
			sessionid = sessions_chg_session(peer->sesdata,exports_checksum(),rootinode,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables,peer->peerip,info,ileng);
		}
	}
	if (peer->sesdata==NULL || sessionid==0) {
		peer->sesdata = sessions_new_session(exports_checksum(),rootinode,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables,peer->peerip,info,ileng);
	}
	if (peer->sesdata==NULL) {
		matoclquic_send_register_status(peer->peerip,peer->peerport,MFS_ERROR_IO);
		return;
	}
	peer->version = version;
	peer->asize = matoclquic_attr_size_for_version(version);
	peer->registered = 1;
	sessions_attach_session(peer->sesdata,peer->peerip,peer->version);
	matoclquic_send_register_ok(peer,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables);
}

static void matoclquic_handle_register(matoclquic_peer *peer,const uint8_t *data,uint32_t leng) {
	uint8_t i;
	uint8_t rcode;

	if (leng<65 || memcmp(data,FUSE_REGISTER_BLOB_ACL,64)!=0) {
		matoclquic_send_register_status(peer->peerip,peer->peerport,MFS_ERROR_EINVAL);
		return;
	}
	rcode = data[64];
	switch (rcode) {
		case REGISTER_GETRANDOM:
			if (leng!=65) {
				matoclquic_send_register_status(peer->peerip,peer->peerport,MFS_ERROR_EINVAL);
				return;
			}
			for (i=0 ; i<32 ; i++) {
				peer->passwordrnd[i] = rndu8();
			}
			matoclquic_send_raw(peer->peerip,peer->peerport,MATOCL_FUSE_REGISTER,peer->passwordrnd,32);
			return;
		case REGISTER_NEWSESSION:
			matoclquic_handle_register_newsession(peer,data,leng);
			return;
		default:
			matoclquic_send_register_status(peer->peerip,peer->peerport,MFS_ERROR_ENOTSUP);
			return;
	}
}

static void matoclquic_handle_lookup(matoclquic_peer *peer,const uint8_t *data,uint32_t leng) {
	const uint8_t *rptr;
	uint8_t *payload;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;
	uint8_t nleng;
	const uint8_t *name;
	uint32_t msgid;
	uint32_t inode;
	uint32_t newinode;
	uint32_t uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint16_t payloadleng;

	if (peer->registered==0 || peer->sesdata==NULL) {
		msgid = (leng>=4) ? get32bit(&data) : 0;
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_LOOKUP,msgid,MFS_ERROR_BADSESSIONID);
		return;
	}
	if (leng<17) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_LOOKUP,0,MFS_ERROR_EINVAL);
		return;
	}
	rptr = data;
	msgid = get32bit(&rptr);
	inode = get32bit(&rptr);
	nleng = get8bit(&rptr);
	if (leng<17U+nleng) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_LOOKUP,msgid,MFS_ERROR_EINVAL);
		return;
	}
	name = rptr;
	rptr += nleng;
	auid = uid = get32bit(&rptr);
	if (leng==17U+nleng) {
		gids = 1;
		gid = malloc(sizeof(uint32_t));
		passert(gid);
		agid = gid[0] = get32bit(&rptr);
		sessions_ugid_remap(peer->sesdata,&uid,gid);
	} else {
		gids = get32bit(&rptr);
		if (gids==0 || leng!=17U+nleng+4*gids) {
			matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_LOOKUP,msgid,MFS_ERROR_EINVAL);
			return;
		}
		gid = malloc(sizeof(uint32_t)*gids);
		passert(gid);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&rptr);
		}
		agid = gid[0];
		sessions_ugid_remap(peer->sesdata,&uid,gid);
	}
	status = fs_lookup(sessions_get_rootinode(peer->sesdata),sessions_get_sesflags(peer->sesdata),inode,nleng,name,uid,gids,gid,auid,agid,&newinode,attr,0,NULL,NULL,NULL,NULL);
	free(gid);
	if (status!=MFS_STATUS_OK) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_LOOKUP,msgid,status);
		return;
	}
	payloadleng = 8 + peer->asize;
	payload = malloc(payloadleng);
	passert(payload);
	rptr = payload;
	put32bit((uint8_t**)&rptr,msgid);
	put32bit((uint8_t**)&rptr,newinode);
	memcpy((uint8_t*)rptr,attr,peer->asize);
	matoclquic_send_raw(peer->peerip,peer->peerport,MATOCL_FUSE_LOOKUP,payload,payloadleng);
	free(payload);
	sessions_inc_stats(peer->sesdata,SES_OP_LOOKUP);
}

static void matoclquic_handle_getattr(matoclquic_peer *peer,const uint8_t *data,uint32_t leng) {
	const uint8_t *rptr;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t *payload;
	uint8_t status;
	uint8_t opened;
	uint32_t msgid;
	uint32_t inode;
	uint32_t uid,gid,auid,agid;
	uint16_t payloadleng;

	if (peer->registered==0 || peer->sesdata==NULL) {
		msgid = (leng>=4) ? get32bit(&data) : 0;
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_GETATTR,msgid,MFS_ERROR_BADSESSIONID);
		return;
	}
	if (leng!=8 && leng!=16 && leng!=17) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_GETATTR,0,MFS_ERROR_EINVAL);
		return;
	}
	rptr = data;
	msgid = get32bit(&rptr);
	inode = get32bit(&rptr);
	opened = (leng==17) ? get8bit(&rptr) : 0;
	if (leng>=16) {
		auid = uid = get32bit(&rptr);
		agid = gid = get32bit(&rptr);
		sessions_ugid_remap(peer->sesdata,&uid,&gid);
	} else {
		auid = uid = 12345;
		agid = gid = 12345;
	}
	status = fs_getattr(sessions_get_rootinode(peer->sesdata),sessions_get_sesflags(peer->sesdata),inode,opened,uid,gid,auid,agid,attr);
	if (status!=MFS_STATUS_OK) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_GETATTR,msgid,status);
		return;
	}
	payloadleng = 4 + peer->asize;
	payload = malloc(payloadleng);
	passert(payload);
	rptr = payload;
	put32bit((uint8_t**)&rptr,msgid);
	memcpy((uint8_t*)rptr,attr,peer->asize);
	matoclquic_send_raw(peer->peerip,peer->peerport,MATOCL_FUSE_GETATTR,payload,payloadleng);
	free(payload);
	sessions_inc_stats(peer->sesdata,SES_OP_GETATTR);
}

static void matoclquic_handle_open(matoclquic_peer *peer,const uint8_t *data,uint32_t leng) {
	const uint8_t *rptr;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t *payload;
	uint8_t status;
	uint8_t flags;
	uint8_t oflags;
	uint8_t sesflags;
	uint32_t msgid;
	uint32_t inode;
	uint32_t uid,gid,auid,agid;
	uint32_t gids;
	uint16_t payloadleng;
	uint8_t knowflags = ((peer->version>=VERSION2INT(3,0,113) && peer->version<VERSION2INT(4,0,0)) || peer->version>=VERSION2INT(4,22,0)) ? 1 : 0;

	if (peer->registered==0 || peer->sesdata==NULL) {
		msgid = (leng>=4) ? get32bit(&data) : 0;
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_OPEN,msgid,MFS_ERROR_BADSESSIONID);
		return;
	}
	if (leng!=21) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_OPEN,0,MFS_ERROR_EINVAL);
		return;
	}
	rptr = data;
	msgid = get32bit(&rptr);
	inode = get32bit(&rptr);
	auid = uid = get32bit(&rptr);
	gids = get32bit(&rptr);
	if (gids!=1) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_OPEN,msgid,MFS_ERROR_EINVAL);
		return;
	}
	agid = gid = get32bit(&rptr);
	sessions_ugid_remap(peer->sesdata,&uid,&gid);
	flags = get8bit(&rptr);
	oflags = 0;
	sesflags = sessions_get_sesflags(peer->sesdata);
	if ((flags&OPEN_TRUNCATE) && (sessions_get_disables(peer->sesdata)&DISABLE_TRUNCATE)) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_opencheck(sessions_get_rootinode(peer->sesdata),sesflags,inode,uid,gids,&gid,auid,agid,flags,attr,&oflags);
	}
	if (status==MFS_STATUS_OK && knowflags==0 && (oflags&OPEN_APPENDONLY) && (flags&OPEN_WRITE)) {
		status = MFS_ERROR_EACCES;
	}
	if (status==MFS_STATUS_OK) {
		of_openfile(sessions_get_id(peer->sesdata),inode);
		if (flags&OPEN_CACHE_CLEARED) {
			dcm_access(inode,sessions_get_id(peer->sesdata));
		}
	}
	if (status!=MFS_STATUS_OK) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_OPEN,msgid,status);
		return;
	}
	if (knowflags) {
		if ((oflags&OPEN_DIRECTMODE)==0) {
			if (dcm_open(inode,sessions_get_id(peer->sesdata))) {
				oflags |= OPEN_KEEPCACHE;
			} else {
				if (sesflags&SESFLAG_ATTRBIT) {
					attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
				} else {
					attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
				}
			}
		}
		payloadleng = peer->asize + 5;
		payload = malloc(payloadleng);
	} else {
		if ((sesflags&SESFLAG_ATTRBIT)==0 || (attr[0]&MATTR_DIRECTMODE)==0) {
			if (dcm_open(inode,sessions_get_id(peer->sesdata))==0) {
				if (sesflags&SESFLAG_ATTRBIT) {
					attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
				} else {
					attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
				}
			}
		}
		payloadleng = peer->asize + 4;
		payload = malloc(payloadleng);
	}
	passert(payload);
	rptr = payload;
	put32bit((uint8_t**)&rptr,msgid);
	if (knowflags) {
		put8bit((uint8_t**)&rptr,oflags);
	}
	memcpy((uint8_t*)rptr,attr,peer->asize);
	matoclquic_send_raw(peer->peerip,peer->peerport,MATOCL_FUSE_OPEN,payload,payloadleng);
	free(payload);
	sessions_inc_stats(peer->sesdata,SES_OP_OPEN);
}

static void matoclquic_handle_create(matoclquic_peer *peer,const uint8_t *data,uint32_t leng) {
	const uint8_t *rptr;
	const uint8_t *name;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t *payload;
	uint8_t status;
	uint8_t nleng;
	uint8_t oflags;
	uint8_t sesflags;
	uint32_t msgid;
	uint32_t inode;
	uint32_t newinode;
	uint32_t uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint16_t mode,cumask;
	uint16_t payloadleng;
	uint8_t knowflags = ((peer->version>=VERSION2INT(3,0,113) && peer->version<VERSION2INT(4,0,0)) || peer->version>=VERSION2INT(4,22,0)) ? 1 : 0;

	if (peer->registered==0 || peer->sesdata==NULL) {
		msgid = (leng>=4) ? get32bit(&data) : 0;
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_CREATE,msgid,MFS_ERROR_BADSESSIONID);
		return;
	}
	if (leng<19) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_CREATE,0,MFS_ERROR_EINVAL);
		return;
	}
	rptr = data;
	msgid = get32bit(&rptr);
	inode = get32bit(&rptr);
	nleng = get8bit(&rptr);
	if (leng!=19U+nleng && leng<21U+nleng) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_CREATE,msgid,MFS_ERROR_EINVAL);
		return;
	}
	name = rptr;
	rptr += nleng;
	mode = get16bit(&rptr);
	if (leng>=21U+nleng) {
		cumask = get16bit(&rptr);
	} else {
		cumask = 0;
	}
	cumask |= sessions_get_umask(peer->sesdata);
	auid = uid = get32bit(&rptr);
	if (leng<=21U+nleng) {
		gids = 1;
		gid = malloc(sizeof(uint32_t));
		passert(gid);
		agid = gid[0] = get32bit(&rptr);
	} else {
		gids = get32bit(&rptr);
		if (gids==0 || leng!=21U+nleng+4*gids) {
			matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_CREATE,msgid,MFS_ERROR_EINVAL);
			return;
		}
		gid = malloc(sizeof(uint32_t)*gids);
		passert(gid);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&rptr);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(peer->sesdata,&uid,gid);
	sesflags = sessions_get_sesflags(peer->sesdata);
	if (sessions_get_disables(peer->sesdata)&DISABLE_CREATE) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_mknod(sessions_get_rootinode(peer->sesdata),sesflags,inode,nleng,name,TYPE_FILE,mode,cumask,uid,gids,gid,auid,agid,0,&newinode,attr,&oflags);
	}
	free(gid);
	if (status!=MFS_STATUS_OK) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_CREATE,msgid,status);
		return;
	}

	of_openfile(sessions_get_id(peer->sesdata),newinode);
	if (knowflags) {
		if ((oflags&OPEN_DIRECTMODE)==0) {
			if (sesflags&SESFLAG_ATTRBIT) {
				attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
			} else {
				attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
			}
		}
		payloadleng = peer->asize + 9;
		payload = malloc(payloadleng);
		passert(payload);
		rptr = payload;
		put32bit((uint8_t**)&rptr,msgid);
		put8bit((uint8_t**)&rptr,oflags);
		put32bit((uint8_t**)&rptr,newinode);
		memcpy((uint8_t*)rptr,attr,peer->asize);
	} else {
		if ((sesflags&SESFLAG_ATTRBIT)==0 || (attr[0]&MATTR_DIRECTMODE)==0) {
			if (sesflags&SESFLAG_ATTRBIT) {
				attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
			} else {
				attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
			}
		}
		payloadleng = peer->asize + 8;
		payload = malloc(payloadleng);
		passert(payload);
		rptr = payload;
		put32bit((uint8_t**)&rptr,msgid);
		put32bit((uint8_t**)&rptr,newinode);
		memcpy((uint8_t*)rptr,attr,peer->asize);
	}
	matoclquic_send_raw(peer->peerip,peer->peerport,MATOCL_FUSE_CREATE,payload,payloadleng);
	free(payload);
	sessions_inc_stats(peer->sesdata,SES_OP_CREATE);
}

static void matoclquic_handle_truncate(matoclquic_peer *peer,const uint8_t *data,uint32_t leng) {
	const uint8_t *rptr;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t *payload;
	uint8_t status;
	uint8_t flags;
	uint32_t msgid;
	uint32_t inode;
	uint32_t uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t disables;
	uint32_t i;
	uint64_t fleng;
	uint64_t prevlength;
	uint16_t payloadleng;
	uint8_t knowflags = ((peer->version>=VERSION2INT(3,0,113) && peer->version<VERSION2INT(4,0,0)) || peer->version>=VERSION2INT(4,22,0)) ? 1 : 0;

	if (peer->registered==0 || peer->sesdata==NULL) {
		msgid = (leng>=4) ? get32bit(&data) : 0;
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_TRUNCATE,msgid,MFS_ERROR_BADSESSIONID);
		return;
	}
	if (leng!=24 && leng<25) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_TRUNCATE,0,MFS_ERROR_EINVAL);
		return;
	}
	rptr = data;
	flags = 0;
	msgid = get32bit(&rptr);
	inode = get32bit(&rptr);
	if (leng>=25) {
		flags = get8bit(&rptr);
	}
	auid = uid = get32bit(&rptr);
	if (leng<=25) {
		gids = 1;
		gid = malloc(sizeof(uint32_t));
		passert(gid);
		agid = gid[0] = get32bit(&rptr);
		if (leng==24 && uid==0 && gid[0]!=0) {
			flags = TRUNCATE_FLAG_OPENED;
		}
	} else {
		gids = get32bit(&rptr);
		if (gids==0 || leng!=25+4*gids) {
			matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_TRUNCATE,msgid,MFS_ERROR_EINVAL);
			return;
		}
		gid = malloc(sizeof(uint32_t)*gids);
		passert(gid);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&rptr);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(peer->sesdata,&uid,gid);
	fleng = get64bit(&rptr);

	status = MFS_STATUS_OK;
	disables = sessions_get_disables(peer->sesdata);
	if (flags & (TRUNCATE_FLAG_RESERVE|TRUNCATE_FLAG_UPDATE)) {
		if (disables&DISABLE_WRITE) {
			status = MFS_ERROR_EPERM;
		}
	} else if ((disables&(DISABLE_TRUNCATE|DISABLE_SETLENGTH))==(DISABLE_TRUNCATE|DISABLE_SETLENGTH)) {
		status = MFS_ERROR_EPERM;
	}
	if (status==MFS_STATUS_OK) {
		uint32_t indx;
		uint64_t prevchunkid;
		uint64_t chunkid;
		status = fs_try_setlength(sessions_get_rootinode(peer->sesdata),sessions_get_sesflags(peer->sesdata),inode,flags,uid,gids,gid,((disables&DISABLE_TRUNCATE)?1:0)|((disables&DISABLE_SETLENGTH)?2:0),fleng,&indx,&prevchunkid,&chunkid);
		if (status==MFS_STATUS_OK) {
			status = fs_do_setlength(sessions_get_rootinode(peer->sesdata),sessions_get_sesflags(peer->sesdata),inode,flags,uid,gid[0],auid,agid,fleng,attr,&prevlength);
		}
	}
	if (status==MFS_STATUS_OK && (flags & TRUNCATE_FLAG_UPDATE)==0) {
		dcm_modify(inode,sessions_get_id(peer->sesdata));
	}
	free(gid);
	if (status!=MFS_STATUS_OK) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_TRUNCATE,msgid,status);
		return;
	}

	if (knowflags) {
		payloadleng = peer->asize + 12;
		payload = malloc(payloadleng);
		passert(payload);
		rptr = payload;
		put32bit((uint8_t**)&rptr,msgid);
		put64bit((uint8_t**)&rptr,prevlength);
		memcpy((uint8_t*)rptr,attr,peer->asize);
	} else {
		payloadleng = peer->asize + 4;
		payload = malloc(payloadleng);
		passert(payload);
		rptr = payload;
		put32bit((uint8_t**)&rptr,msgid);
		memcpy((uint8_t*)rptr,attr,peer->asize);
	}
	matoclquic_send_raw(peer->peerip,peer->peerport,MATOCL_FUSE_TRUNCATE,payload,payloadleng);
	free(payload);
	sessions_inc_stats(peer->sesdata,SES_OP_TRUNCATE);
}

static void matoclquic_handle_write_chunk(matoclquic_peer *peer,const uint8_t *data,uint32_t leng) {
	const uint8_t *rptr;
	uint8_t *payload;
	uint8_t status;
	uint8_t chunkopflags;
	uint8_t split;
	uint8_t count;
	uint8_t cs_data[100*14];
	uint32_t msgid;
	uint32_t inode;
	uint32_t indx;
	uint32_t version;
	uint16_t payloadleng;
	uint64_t fleng;
	uint64_t prevchunkid;
	uint64_t chunkid;
	uint8_t opflag;
	int attempt;

	if (peer->registered==0 || peer->sesdata==NULL) {
		msgid = (leng>=4) ? get32bit(&data) : 0;
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_WRITE_CHUNK,msgid,MFS_ERROR_BADSESSIONID);
		return;
	}
	if (leng!=12 && leng!=13) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_WRITE_CHUNK,0,MFS_ERROR_EINVAL);
		return;
	}
	rptr = data;
	msgid = get32bit(&rptr);
	inode = get32bit(&rptr);
	indx = get32bit(&rptr);
	if (leng>=13) {
		chunkopflags = get8bit(&rptr);
	} else {
		chunkopflags = CHUNKOPFLAG_CANMODTIME;
	}
	if (peer->version>=VERSION2INT(3,0,74)) {
		chunkopflags &= ~CHUNKOPFLAG_CANMODTIME;
	}

	if (sessions_get_disables(peer->sesdata)&DISABLE_WRITE) {
		status = MFS_ERROR_EPERM;
	} else if (sessions_get_sesflags(peer->sesdata)&SESFLAG_READONLY) {
		status = (peer->version>=VERSION2INT(3,0,101)) ? MFS_ERROR_EROFS : MFS_ERROR_IO;
	} else {
		status = fs_writechunk(inode,indx,chunkopflags,&prevchunkid,&chunkid,&fleng,&opflag,peer->peerip);
	}
	if (status!=MFS_STATUS_OK) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_WRITE_CHUNK,msgid,status);
		return;
	}
	sessions_inc_stats(peer->sesdata,SES_OP_WRITECHUNK);
	dcm_modify(inode,sessions_get_id(peer->sesdata));

	status = MFS_ERROR_EAGAIN;
	for (attempt=0 ; attempt<100 ; attempt++) {
		if (peer->version>=VERSION2INT(3,0,10)) {
			status = chunk_get_version_and_csdata(2,chunkid,peer->peerip,&version,&count,cs_data,&split);
		} else if (peer->version>=VERSION2INT(1,7,32)) {
			status = chunk_get_version_and_csdata(1,chunkid,peer->peerip,&version,&count,cs_data,&split);
		} else {
			status = chunk_get_version_and_csdata(0,chunkid,peer->peerip,&version,&count,cs_data,&split);
		}
		if (status==MFS_STATUS_OK && split==0) {
			break;
		}
		usleep(10000);
	}
	if (status!=MFS_STATUS_OK || split) {
		if (status==MFS_STATUS_OK && split) {
			status = MFS_ERROR_EAGAIN;
		}
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_WRITE_CHUNK,msgid,status);
		fs_writeend(0,0,chunkid,0,NULL);
		return;
	}

	if (peer->version>=VERSION2INT(3,0,10)) {
		payloadleng = 25 + count*14;
		payload = malloc(payloadleng);
	} else if (peer->version>=VERSION2INT(1,7,32)) {
		payloadleng = 25 + count*10;
		payload = malloc(payloadleng);
	} else {
		payloadleng = 24 + count*6;
		payload = malloc(payloadleng);
	}
	passert(payload);
	rptr = payload;
	put32bit((uint8_t**)&rptr,msgid);
	if (peer->version>=VERSION2INT(3,0,10)) {
		put8bit((uint8_t**)&rptr,2);
	} else if (peer->version>=VERSION2INT(1,7,32)) {
		put8bit((uint8_t**)&rptr,1);
	}
	put64bit((uint8_t**)&rptr,fleng);
	put64bit((uint8_t**)&rptr,chunkid);
	put32bit((uint8_t**)&rptr,version);
	if (count>0) {
		if (peer->version>=VERSION2INT(3,0,10)) {
			memcpy((uint8_t*)rptr,cs_data,count*14);
		} else if (peer->version>=VERSION2INT(1,7,32)) {
			memcpy((uint8_t*)rptr,cs_data,count*10);
		} else {
			memcpy((uint8_t*)rptr,cs_data,count*6);
		}
	}
	matoclquic_send_raw(peer->peerip,peer->peerport,MATOCL_FUSE_WRITE_CHUNK,payload,payloadleng);
	free(payload);
}

static void matoclquic_handle_write_chunk_end(matoclquic_peer *peer,const uint8_t *data,uint32_t leng) {
	const uint8_t *rptr;
	uint8_t payload[5];
	uint8_t *ptr;
	uint8_t status;
	uint8_t chunkopflags;
	uint8_t flenghaschanged;
	uint32_t msgid;
	uint32_t inode;
	uint32_t indx;
	uint32_t offset;
	uint32_t size;
	uint64_t fleng;
	uint64_t chunkid;

	if (peer->registered==0 || peer->sesdata==NULL) {
		msgid = (leng>=4) ? get32bit(&data) : 0;
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_WRITE_CHUNK_END,msgid,MFS_ERROR_BADSESSIONID);
		return;
	}
	if (leng!=24 && leng!=25 && leng!=29 && leng!=37) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_WRITE_CHUNK_END,0,MFS_ERROR_EINVAL);
		return;
	}
	rptr = data;
	msgid = get32bit(&rptr);
	chunkid = get64bit(&rptr);
	inode = get32bit(&rptr);
	if (leng>=29) {
		indx = get32bit(&rptr);
	} else {
		indx = 0;
	}
	fleng = get64bit(&rptr);
	if (leng>=25) {
		chunkopflags = get8bit(&rptr);
	} else {
		chunkopflags = CHUNKOPFLAG_CANMODTIME;
	}
	if (leng>=37) {
		if (peer->version<VERSION2INT(4,48,0)) {
			offset = 0;
			size = MFSCHUNKSIZE;
			rptr += 8;
		} else {
			offset = get32bit(&rptr);
			size = get32bit(&rptr);
		}
	} else {
		offset = 0;
		size = MFSCHUNKSIZE;
	}
	if (peer->version>=VERSION2INT(3,0,74)) {
		chunkopflags &= ~CHUNKOPFLAG_CANMODTIME;
	}
	flenghaschanged = 0;
	if (sessions_get_disables(peer->sesdata)&DISABLE_WRITE) {
		status = MFS_ERROR_EPERM;
	} else if (sessions_get_sesflags(peer->sesdata)&SESFLAG_READONLY) {
		if (peer->version>=VERSION2INT(3,0,101)) {
			status = MFS_ERROR_EROFS;
		} else {
			status = MFS_ERROR_IO;
		}
	} else {
		status = fs_writeend(inode,fleng,chunkid,chunkopflags,&flenghaschanged);
	}
	dcm_modify(inode,sessions_get_id(peer->sesdata));
	ptr = payload;
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	matoclquic_send_raw(peer->peerip,peer->peerport,MATOCL_FUSE_WRITE_CHUNK_END,payload,sizeof(payload));
	(void)indx;
	(void)offset;
	(void)size;
	(void)flenghaschanged;
}

static void matoclquic_handle_read_chunk(matoclquic_peer *peer,const uint8_t *data,uint32_t leng) {
	const uint8_t *rptr;
	uint8_t *payload;
	uint8_t status;
	uint8_t chunkopflags;
	uint8_t split;
	uint8_t count;
	uint8_t cs_data[100*14];
	uint32_t msgid;
	uint32_t inode;
	uint32_t indx;
	uint32_t version;
	uint16_t payloadleng;
	uint64_t chunkid;
	uint64_t fleng;

	if (peer->registered==0 || peer->sesdata==NULL) {
		msgid = (leng>=4) ? get32bit(&data) : 0;
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_READ_CHUNK,msgid,MFS_ERROR_BADSESSIONID);
		return;
	}
	if (leng!=12 && leng!=13) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_READ_CHUNK,0,MFS_ERROR_EINVAL);
		return;
	}
	rptr = data;
	msgid = get32bit(&rptr);
	inode = get32bit(&rptr);
	indx = get32bit(&rptr);
	if (leng==13) {
		chunkopflags = get8bit(&rptr);
	} else {
		chunkopflags = CHUNKOPFLAG_CANMODTIME;
	}
	if (peer->version>=VERSION2INT(3,0,74)) {
		chunkopflags &= ~CHUNKOPFLAG_CANMODTIME;
	}
	if (sessions_get_disables(peer->sesdata)&DISABLE_READ) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_readchunk(inode,sessions_get_sesflags(peer->sesdata),indx,chunkopflags,1,&chunkid,&fleng);
	}
	if (status!=MFS_STATUS_OK) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_READ_CHUNK,msgid,status);
		return;
	}
	sessions_inc_stats(peer->sesdata,SES_OP_READCHUNK);
	split = 0;
	if (chunkid>0) {
		if (peer->version>=VERSION2INT(3,0,10)) {
			status = chunk_get_version_and_csdata(2,chunkid,peer->peerip,&version,&count,cs_data,&split);
		} else if (peer->version>=VERSION2INT(1,7,32)) {
			status = chunk_get_version_and_csdata(1,chunkid,peer->peerip,&version,&count,cs_data,&split);
		} else {
			status = chunk_get_version_and_csdata(0,chunkid,peer->peerip,&version,&count,cs_data,&split);
		}
		if (status==MFS_STATUS_OK && count==0 && split==0 && version==0) {
			chunkid = 0;
		}
	} else {
		version = 0;
		count = 0;
	}
	if ((peer->version<VERSION2INT(4,0,0) && split==8) || ((peer->version<VERSION2INT(4,26,0) && split==4))) {
		status = MFS_ERROR_IO;
	}
	if (status!=MFS_STATUS_OK) {
		matoclquic_send_msg_status(peer->peerip,peer->peerport,MATOCL_FUSE_READ_CHUNK,msgid,status);
		return;
	}
	dcm_access(inode,sessions_get_id(peer->sesdata));
	if (peer->version>=VERSION2INT(3,0,10)) {
		payloadleng = 25 + count*14;
	} else if (peer->version>=VERSION2INT(1,7,32)) {
		payloadleng = 25 + count*10;
	} else {
		payloadleng = 24 + count*6;
	}
	payload = malloc(payloadleng);
	passert(payload);
	rptr = payload;
	put32bit((uint8_t**)&rptr,msgid);
	if (peer->version>=VERSION2INT(3,0,10)) {
		put8bit((uint8_t**)&rptr,split?3:2);
	} else if (peer->version>=VERSION2INT(1,7,32)) {
		put8bit((uint8_t**)&rptr,1);
	}
	put64bit((uint8_t**)&rptr,fleng);
	put64bit((uint8_t**)&rptr,chunkid);
	put32bit((uint8_t**)&rptr,version);
	if (count>0) {
		if (peer->version>=VERSION2INT(3,0,10)) {
			memcpy((uint8_t*)rptr,cs_data,count*14);
		} else if (peer->version>=VERSION2INT(1,7,32)) {
			memcpy((uint8_t*)rptr,cs_data,count*10);
		} else {
			memcpy((uint8_t*)rptr,cs_data,count*6);
		}
	}
	matoclquic_send_raw(peer->peerip,peer->peerport,MATOCL_FUSE_READ_CHUNK,payload,payloadleng);
	free(payload);
}

static void matoclquic_handle_hello(uint32_t peerip,uint16_t peerport,const uint8_t *buff,uint32_t leng) {
	const uint8_t *ptr;
	uint32_t magic;
	uint32_t version;
	uint32_t flags;
	uint8_t alpnleng;
	size_t serveralpnleng;

	if (leng<13) {
		matoclquic_send_hello(peerip,peerport,MFS_ERROR_EINVAL,0,NULL,0);
		return;
	}
	ptr = buff;
	magic = get32bit(&ptr);
	version = get32bit(&ptr);
	flags = get32bit(&ptr);
	alpnleng = get8bit(&ptr);
	if (magic!=MATOCLQUIC_MAGIC || leng!=(uint32_t)(13+alpnleng)) {
		matoclquic_send_hello(peerip,peerport,MFS_ERROR_EINVAL,0,NULL,0);
		return;
	}
	serveralpnleng = strlen(QuicAlpn);
	if (serveralpnleng>255) {
		serveralpnleng = 255;
	}
	if (alpnleng!=serveralpnleng || memcmp(ptr,QuicAlpn,alpnleng)!=0) {
		matoclquic_send_hello(peerip,peerport,MFS_ERROR_ENOTSUP,0,(const uint8_t*)QuicAlpn,(uint8_t)serveralpnleng);
		return;
	}
	(void)version;
	matoclquic_get_peer(peerip,peerport);
	matoclquic_send_hello(peerip,peerport,MFS_STATUS_OK,
		MATOCLQUIC_FLAG_PACKET_MODE |
		MATOCLQUIC_FLAG_TCP_FALLBACK |
		((flags&MATOCLQUIC_FLAG_ZERO_RTT)?MATOCLQUIC_FLAG_ZERO_RTT:0) |
		MATOCLQUIC_FLAG_DATAGRAMS |
		((QuicBackend==MATOCLQUIC_BACKEND_REALQUIC)?MATOCLQUIC_FLAG_TLS:0),
		(const uint8_t*)QuicAlpn,(uint8_t)serveralpnleng);
}

static void matoclquic_dispatch(uint32_t peerip,uint16_t peerport,uint32_t packettype,const uint8_t *payload,uint32_t payloadleng) {
	matoclquic_peer *peer;

	if (packettype==CLTOMA_QUIC_HELLO) {
		matoclquic_handle_hello(peerip,peerport,payload,payloadleng);
		return;
	}
	peer = matoclquic_get_peer(peerip,peerport);
	switch (packettype) {
		case CLTOMA_FUSE_REGISTER:
			matoclquic_handle_register(peer,payload,payloadleng);
			break;
		case CLTOMA_FUSE_LOOKUP:
			matoclquic_handle_lookup(peer,payload,payloadleng);
			break;
		case CLTOMA_FUSE_GETATTR:
			matoclquic_handle_getattr(peer,payload,payloadleng);
			break;
		case CLTOMA_FUSE_OPEN:
			matoclquic_handle_open(peer,payload,payloadleng);
			break;
		case CLTOMA_FUSE_CREATE:
			matoclquic_handle_create(peer,payload,payloadleng);
			break;
		case CLTOMA_FUSE_TRUNCATE:
			matoclquic_handle_truncate(peer,payload,payloadleng);
			break;
		case CLTOMA_FUSE_WRITE_CHUNK:
			matoclquic_handle_write_chunk(peer,payload,payloadleng);
			break;
		case CLTOMA_FUSE_WRITE_CHUNK_END:
			matoclquic_handle_write_chunk_end(peer,payload,payloadleng);
			break;
		case CLTOMA_FUSE_READ_CHUNK:
			matoclquic_handle_read_chunk(peer,payload,payloadleng);
			break;
		default:
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"main master QUIC module: unsupported native packet %"PRIu32" from %s",packettype,univallocstripport(peerip,peerport));
			break;
	}
}

static void matoclquic_dispatch_realquic(uint32_t peerip,uint16_t peerport,const uint8_t *payload,uint32_t payloadleng) {
#ifdef HAVE_NGTCP2
	matoclquic_peer *peer;
	const uint8_t *ptr;
	uint32_t packettype;
	uint32_t packetleng;
	if (payloadleng>=8) {
		ptr = payload;
		packettype = get32bit(&ptr);
		packetleng = get32bit(&ptr);
		if (packetleng==payloadleng-8 && packettype==CLTOMA_QUIC_HELLO) {
			matoclquic_dispatch(peerip,peerport,packettype,ptr,packetleng);
			return;
		}
	}
	peer = matoclquic_get_peer(peerip,peerport);
	matoclquic_handle_realquic_datagram(peer,payload,payloadleng);
#else
	(void)peerip;
	(void)peerport;
	(void)payload;
	(void)payloadleng;
#endif
}

static void matoclquic_close_socket(void) {
	if (lsock>=0) {
		udpclose(lsock);
		lsock = -1;
	}
	lsockpdescpos = -1;
}

static int matoclquic_open_socket(void) {
	int newlsock;
	uint32_t newlistenip;
	uint16_t newlistenport;

	if (QuicBackend==MATOCLQUIC_BACKEND_REALQUIC) {
		if (matoclquic_init_realquic_runtime()<0) {
			return -1;
		}
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
			"main master QUIC module: backend '%s' initialized TLS runtime, but packet processing is still experimental",
			matoclquic_backend_name(QuicBackend));
	}

	newlsock = udpsocket();
	if (newlsock<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"main master QUIC module: can't create UDP socket");
		return -1;
	}
	udpnonblock(newlsock);
	if (udpresolve(QuicListenHost,QuicListenPort,&newlistenip,&newlistenport,1)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"main master QUIC module: can't resolve %s:%s",QuicListenHost,QuicListenPort);
		udpclose(newlsock);
		return -1;
	}
	if (udpnumlisten(newlsock,newlistenip,newlistenport)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"main master QUIC module: can't listen on %s:%s",QuicListenHost,QuicListenPort);
		udpclose(newlsock);
		return -1;
	}
	matoclquic_close_socket();
	lsock = newlsock;
	listenip = newlistenip;
	listenport = newlistenport;
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,
		"main master QUIC module: listen on %s:%s (backend: %s, ALPN: %s)",
		QuicListenHost,QuicListenPort,matoclquic_backend_name(QuicBackend),QuicAlpn);
	return 0;
}

static void matoclquic_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	if (lsock>=0) {
		lsockpdescpos = *ndesc;
		pdesc[lsockpdescpos].fd = lsock;
		pdesc[lsockpdescpos].events = POLLIN;
		(*ndesc)++;
	} else {
		lsockpdescpos = -1;
	}
}

static void matoclquic_serve(struct pollfd *pdesc) {
	uint8_t buff[MATOCLQUIC_RECV_BUFFER_SIZE];
	const uint8_t *ptr;
	uint32_t peerip;
	uint16_t peerport;
	uint32_t packettype;
	uint32_t packetleng;
	int rcvd;

	if (lsock<0 || lsockpdescpos<0) {
		return;
	}
	if ((pdesc[lsockpdescpos].revents & (POLLERR|POLLHUP))!=0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"main master QUIC module: UDP listener reported poll error, closing socket");
		matoclquic_close_socket();
		return;
	}
	if ((pdesc[lsockpdescpos].revents & POLLIN)==0) {
		return;
	}
	for (;;) {
		rcvd = udpread(lsock,&peerip,&peerport,buff,MATOCLQUIC_RECV_BUFFER_SIZE);
		if (rcvd<0) {
			if (ERRNO_ERROR) {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_NOTICE,"main master QUIC module: UDP receive error");
			}
			break;
		}
		if (rcvd==0) {
			break;
		}
		QuicDatagramsReceived++;
		QuicBytesReceived += (uint32_t)rcvd;
		if (QuicBackend==MATOCLQUIC_BACKEND_REALQUIC) {
			matoclquic_dispatch_realquic(peerip,peerport,buff,(uint32_t)rcvd);
			continue;
		}
		if (rcvd<8) {
			continue;
		}
		ptr = buff;
		packettype = get32bit(&ptr);
		packetleng = get32bit(&ptr);
		if (packetleng!=(uint32_t)(rcvd-8)) {
			continue;
		}
		matoclquic_dispatch(peerip,peerport,packettype,ptr,packetleng);
	}
}

static void matoclquic_reload(void) {
	uint8_t newenabled;
	uint8_t newbackend;
	char *newhost;
	char *newport;
	char *newalpn;
	char *newcert;
	char *newkey;
	char *backendstr;

	newenabled = cfg_getuint8("MATOCL_QUIC_ENABLE",0);
	backendstr = cfg_getstr("MATOCL_QUIC_BACKEND","datagram");
	newbackend = matoclquic_backend_from_string(backendstr);
	newhost = cfg_getstr("MATOCL_QUIC_LISTEN_HOST","*");
	newport = cfg_getstr("MATOCL_QUIC_LISTEN_PORT","9423");
	newalpn = cfg_getstr("MATOCL_QUIC_ALPN","mfs-direct/exp1");
	newcert = cfg_getstr("MATOCL_QUIC_CERT_FILE","");
	newkey = cfg_getstr("MATOCL_QUIC_KEY_FILE","");
	TcpListenPort = cfg_getuint32("MATOCL_LISTEN_PORT",9421);
	free(backendstr);

	if (QuicListenHost!=NULL) {
		free(QuicListenHost);
	}
	if (QuicListenPort!=NULL) {
		free(QuicListenPort);
	}
	if (QuicAlpn!=NULL) {
		free(QuicAlpn);
	}
	if (QuicCertFile!=NULL) {
		free(QuicCertFile);
	}
	if (QuicKeyFile!=NULL) {
		free(QuicKeyFile);
	}
	matoclquic_close_runtime();

	QuicEnabled = newenabled;
	QuicBackend = newbackend;
	QuicListenHost = newhost;
	QuicListenPort = newport;
	QuicAlpn = newalpn;
	QuicCertFile = newcert;
	QuicKeyFile = newkey;

	if (QuicEnabled) {
		if (matoclquic_open_socket()<0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,
				"main master QUIC module: failed to activate backend '%s' for %s:%s (ALPN: %s)",
				matoclquic_backend_name(QuicBackend),QuicListenHost,QuicListenPort,QuicAlpn);
		}
	} else {
		matoclquic_close_socket();
	}
}

void matoclquic_close_lsock(void) {
	if (lsock>=0) {
		close(lsock);
		lsock = -1;
	}
	lsockpdescpos = -1;
}

static void matoclquic_term(void) {
	matoclquic_peer *peer;
	matoclquic_peer *next;

	for (peer=quicpeerhead ; peer ; peer=next) {
		next = peer->next;
		if (peer->sesdata!=NULL) {
			sessions_disconnection(peer->sesdata);
		}
#ifdef HAVE_NGTCP2
		matoclquic_peer_free_realquic(peer);
#endif
		free(peer);
	}
	quicpeerhead = NULL;
	matoclquic_close_socket();
	if (QuicListenHost!=NULL) {
		free(QuicListenHost);
		QuicListenHost = NULL;
	}
	if (QuicListenPort!=NULL) {
		free(QuicListenPort);
		QuicListenPort = NULL;
	}
	if (QuicAlpn!=NULL) {
		free(QuicAlpn);
		QuicAlpn = NULL;
	}
	if (QuicCertFile!=NULL) {
		free(QuicCertFile);
		QuicCertFile = NULL;
	}
	if (QuicKeyFile!=NULL) {
		free(QuicKeyFile);
		QuicKeyFile = NULL;
	}
	matoclquic_close_runtime();
	QuicEnabled = 0;
	QuicBackend = MATOCLQUIC_BACKEND_DATAGRAM;
}

int matoclquic_init(void) {
	QuicEnabled = 0;
	QuicBackend = MATOCLQUIC_BACKEND_DATAGRAM;
	QuicListenHost = NULL;
	QuicListenPort = NULL;
	QuicAlpn = NULL;
	QuicCertFile = NULL;
	QuicKeyFile = NULL;
#ifdef HAVE_NGTCP2
	QuicSslCtx = NULL;
#endif
	TcpListenPort = 9421;
	QuicDatagramsReceived = 0;
	QuicBytesReceived = 0;
	quicpeerhead = NULL;

	matoclquic_reload();
	main_reload_register(matoclquic_reload);
	main_destruct_register(matoclquic_term);
	main_poll_register(matoclquic_desc,matoclquic_serve);
	return 0;
}
