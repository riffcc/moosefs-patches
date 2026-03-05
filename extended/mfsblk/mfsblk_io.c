#include <linux/blk_types.h>
#include <linux/errno.h>
#include <linux/highmem.h>
#include <linux/kernel.h>
#include <linux/math64.h>
#include <linux/mm.h>
#include <linux/minmax.h>
#include <linux/slab.h>
#include <linux/vmalloc.h>

#include "mfsblk.h"

struct mfsblk_io_work {
	struct work_struct work;
	struct mfsblk_dev *dev;
	struct request *rq;
};

static blk_status_t mfsblk_errno_to_sts(int err)
{
	err = err < 0 ? -err : err;
	switch (err) {
	case 0:
		return BLK_STS_OK;
	case ENOMEM:
		return BLK_STS_RESOURCE;
	case EOPNOTSUPP:
		return BLK_STS_NOTSUPP;
	case ENOSPC:
		return BLK_STS_NOSPC;
	default:
		return BLK_STS_IOERR;
	}
}

static void mfsblk_copy_from_bvec(void *dst, const struct bio_vec *bvec)
{
	struct page *page = bvec->bv_page;
	size_t offset = bvec->bv_offset;
	size_t left = bvec->bv_len;
	u8 *out = dst;

	while (left) {
		size_t take = min_t(size_t, left, PAGE_SIZE - offset);
		void *kaddr = kmap_local_page(page);

		memcpy(out, (u8 *)kaddr + offset, take);
		kunmap_local(kaddr);

		out += take;
		left -= take;
		offset = 0;
		page++;
	}
}

static void mfsblk_copy_to_bvec(struct bio_vec *bvec, const void *src)
{
	struct page *page = bvec->bv_page;
	size_t offset = bvec->bv_offset;
	size_t left = bvec->bv_len;
	const u8 *in = src;

	while (left) {
		size_t take = min_t(size_t, left, PAGE_SIZE - offset);
		void *kaddr = kmap_local_page(page);

		memcpy((u8 *)kaddr + offset, in, take);
		kunmap_local(kaddr);

		in += take;
		left -= take;
		offset = 0;
		page++;
	}
}

int mfsblk_io_rw(struct mfsblk_dev *dev, bool write, u64 offset, void *buffer,
		u32 len)
{
	u64 end;
	u64 done = 0;
	u8 *ptr = buffer;

	if (check_add_overflow(offset, (u64)len, &end))
		return -ERANGE;

	if (offset >= dev->size_bytes) {
		if (write)
			return -ENOSPC;
		memset(buffer, 0, len);
		return 0;
	}

	if (end > dev->size_bytes) {
		u64 clipped = dev->size_bytes - offset;

		if (write)
			len = clipped;
		else
			memset(ptr + clipped, 0, len - clipped);
	}

	while (done < len) {
		struct mfsblk_chunk_desc chunk;
		u64 abs = offset + done;
		u64 chunk_idx = div_u64(abs, MFSBLK_CHUNK_SIZE);
		u32 chunk_off = do_div(abs, MFSBLK_CHUNK_SIZE);
		u32 part = min_t(u32, len - done, MFSBLK_CHUNK_SIZE - chunk_off);
		int ret;

		ret = mfsblk_cache_get_chunk(dev, chunk_idx, write, &chunk);
		if (ret)
			return ret;

		if (!chunk.chunk_id) {
			if (write)
				return -ENODATA;
			memset(ptr + done, 0, part);
			done += part;
			continue;
		}

		if (write) {
			ret = mfsblk_conn_chunk_write(dev, &chunk, chunk_off,
						      ptr + done, part);
			if (ret)
				return ret;
		} else {
			ret = mfsblk_conn_chunk_read(dev, &chunk, chunk_off,
						     ptr + done, part);
			if (ret)
				return ret;
		}

		done += part;
	}

	return 0;
}

int mfsblk_io_discard(struct mfsblk_dev *dev, u64 offset, u64 len)
{
	u8 *zero;
	u64 done = 0;
	int ret;

	ret = mfsblk_conn_trim(dev, offset, len);
	if (!ret)
		return 0;

	zero = vzalloc(MFSBLK_BLOCK_SIZE);
	if (!zero)
		return -ENOMEM;

	while (done < len) {
		u32 part = min_t(u64, MFSBLK_BLOCK_SIZE, len - done);

		ret = mfsblk_io_rw(dev, true, offset + done, zero, part);
		if (ret)
			break;
		done += part;
	}

	vfree(zero);
	return ret;
}

static int mfsblk_handle_rw_rq(struct mfsblk_dev *dev, struct request *rq,
			       bool write)
{
	struct req_iterator iter;
	struct bio_vec bvec;
	u64 pos = (u64)blk_rq_pos(rq) << MFSBLK_SECTOR_SHIFT;
	int ret = 0;

	rq_for_each_segment(bvec, rq, iter) {
		u8 *bounce;

		bounce = kmalloc(bvec.bv_len, GFP_NOIO);
		if (!bounce)
			return -ENOMEM;

		if (write)
			mfsblk_copy_from_bvec(bounce, &bvec);

		ret = mfsblk_io_rw(dev, write, pos, bounce, bvec.bv_len);
		if (!ret && !write)
			mfsblk_copy_to_bvec(&bvec, bounce);

		kfree(bounce);
		if (ret)
			return ret;
		pos += bvec.bv_len;
	}

	return 0;
}

static int mfsblk_handle_rq(struct mfsblk_dev *dev, struct request *rq)
{
	int ret;
	unsigned int op = req_op(rq);

	switch (op) {
	case REQ_OP_READ:
		atomic64_inc(&dev->stats.read_reqs);
		ret = mfsblk_handle_rw_rq(dev, rq, false);
		if (!ret)
			atomic64_add(blk_rq_bytes(rq), &dev->stats.read_bytes);
		return ret;
	case REQ_OP_WRITE:
		atomic64_inc(&dev->stats.write_reqs);
		ret = mfsblk_handle_rw_rq(dev, rq, true);
		if (!ret)
			atomic64_add(blk_rq_bytes(rq), &dev->stats.write_bytes);
		return ret;
	case REQ_OP_DISCARD:
		atomic64_inc(&dev->stats.trim_reqs);
		return mfsblk_io_discard(dev,
			(u64)blk_rq_pos(rq) << MFSBLK_SECTOR_SHIFT,
			blk_rq_bytes(rq));
	case REQ_OP_FLUSH:
		return 0;
	default:
		return -EOPNOTSUPP;
	}
}

static void mfsblk_rq_workfn(struct work_struct *work)
{
	struct mfsblk_io_work *io_work =
		container_of(work, struct mfsblk_io_work, work);
	struct mfsblk_dev *dev = io_work->dev;
	struct request *rq = io_work->rq;
	int ret;

	ret = mfsblk_handle_rq(dev, rq);
	if (ret)
		atomic64_inc(&dev->stats.errors);
	atomic_dec(&dev->stats.inflight);

	blk_mq_end_request(rq, mfsblk_errno_to_sts(ret));
	kfree(io_work);
}

static blk_status_t mfsblk_queue_rq(struct blk_mq_hw_ctx *hctx,
				    const struct blk_mq_queue_data *bd)
{
	struct request *rq = bd->rq;
	struct mfsblk_dev *dev = rq->q->queuedata;
	struct mfsblk_io_work *io_work;

	if (!dev || READ_ONCE(dev->removing))
		return BLK_STS_IOERR;

	io_work = kmalloc(sizeof(*io_work), GFP_ATOMIC);
	if (!io_work)
		return BLK_STS_RESOURCE;

	atomic_inc(&dev->stats.inflight);
	blk_mq_start_request(rq);

	INIT_WORK(&io_work->work, mfsblk_rq_workfn);
	io_work->dev = dev;
	io_work->rq = rq;

	if (!queue_work(dev->io_wq, &io_work->work)) {
		atomic_dec(&dev->stats.inflight);
		blk_mq_end_request(rq, BLK_STS_RESOURCE);
		kfree(io_work);
	}

	return BLK_STS_OK;
}

const struct blk_mq_ops mfsblk_mq_ops = {
	.queue_rq = mfsblk_queue_rq,
};
