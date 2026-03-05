#include "mfs.h"

#include <linux/miscdevice.h>
#include <linux/uaccess.h>
#include <linux/poll.h>

struct mfs_pending_req {
	struct list_head tx_node;
	struct list_head inflight_node;
	struct completion done;
	u32 req_id;
	u16 op;
	void *payload;
	u32 payload_len;
	void *resp;
	u32 resp_len;
	s32 status;
	bool done_flag;
};

static LIST_HEAD(mfs_tx_queue);
static LIST_HEAD(mfs_inflight);
static DEFINE_MUTEX(mfs_ctrl_lock);
static DECLARE_WAIT_QUEUE_HEAD(mfs_ctrl_wq);
static atomic_t mfs_next_req = ATOMIC_INIT(1);
static int mfs_helper_openers;

static void mfs_fail_all_locked(s32 status)
{
	struct mfs_pending_req *req;
	struct mfs_pending_req *tmp;

	list_for_each_entry_safe(req, tmp, &mfs_inflight, inflight_node) {
		if (!list_empty(&req->tx_node))
			list_del_init(&req->tx_node);
		list_del_init(&req->inflight_node);
		kfree(req->resp);
		req->resp = NULL;
		req->resp_len = 0;
		req->status = status;
		req->done_flag = true;
		complete_all(&req->done);
	}
}

bool mfs_helper_is_online(void)
{
	bool online;

	mutex_lock(&mfs_ctrl_lock);
	online = mfs_helper_openers > 0;
	mutex_unlock(&mfs_ctrl_lock);
	return online;
}

static struct mfs_pending_req *mfs_find_inflight_locked(u32 req_id)
{
	struct mfs_pending_req *req;

	list_for_each_entry(req, &mfs_inflight, inflight_node) {
		if (req->req_id == req_id)
			return req;
	}
	return NULL;
}

int mfs_helper_call(u16 op, const void *req_data, u32 req_len,
		    void **rsp, u32 *rsp_len, s32 *status,
		    unsigned long timeout)
{
	struct mfs_pending_req *req;
	unsigned long left;
	int ret = 0;

	if (!rsp || !rsp_len || !status)
		return -EINVAL;
	if (req_len > MFS_CTRL_MAX_PAYLOAD)
		return -EMSGSIZE;

	rsp_len[0] = 0;
	rsp[0] = NULL;
	status[0] = -EIO;

	req = kzalloc(sizeof(*req), GFP_KERNEL);
	if (!req)
		return -ENOMEM;

	if (req_len) {
		req->payload = kmemdup(req_data, req_len, GFP_KERNEL);
		if (!req->payload) {
			kfree(req);
			return -ENOMEM;
		}
	}
	INIT_LIST_HEAD(&req->tx_node);
	INIT_LIST_HEAD(&req->inflight_node);
	init_completion(&req->done);
	req->op = op;
	req->payload_len = req_len;
	req->req_id = (u32)atomic_inc_return(&mfs_next_req);

	mutex_lock(&mfs_ctrl_lock);
	if (mfs_helper_openers <= 0) {
		ret = -ENOTCONN;
		mutex_unlock(&mfs_ctrl_lock);
		goto out;
	}
	list_add_tail(&req->tx_node, &mfs_tx_queue);
	list_add_tail(&req->inflight_node, &mfs_inflight);
	mutex_unlock(&mfs_ctrl_lock);

	wake_up_interruptible(&mfs_ctrl_wq);

	left = wait_for_completion_interruptible_timeout(&req->done, timeout);
	if (left == 0) {
		mutex_lock(&mfs_ctrl_lock);
		if (!req->done_flag) {
			if (!list_empty(&req->tx_node))
				list_del_init(&req->tx_node);
			if (!list_empty(&req->inflight_node))
				list_del_init(&req->inflight_node);
			req->status = -ETIMEDOUT;
			req->done_flag = true;
		}
		mutex_unlock(&mfs_ctrl_lock);
		ret = -ETIMEDOUT;
		goto out;
	}
	if (left < 0) {
		ret = (int)left;
		goto out;
	}

	status[0] = req->status;
	rsp_len[0] = req->resp_len;
	rsp[0] = req->resp;
	req->resp = NULL;

out:
	kfree(req->resp);
	kfree(req->payload);
	kfree(req);
	return ret;
}

static ssize_t mfs_ctrl_read(struct file *file, char __user *buf,
			     size_t count, loff_t *ppos)
{
	struct mfs_pending_req *req;
	struct mfs_ctrl_hdr hdr;
	size_t total;
	int err;

	(void)file;
	(void)ppos;

	if (count < sizeof(hdr))
		return -EINVAL;

retry:
	err = wait_event_interruptible(mfs_ctrl_wq,
				       !list_empty(&mfs_tx_queue) ||
				       mfs_helper_openers <= 0);
	if (err)
		return err;

	mutex_lock(&mfs_ctrl_lock);
	if (list_empty(&mfs_tx_queue)) {
		mutex_unlock(&mfs_ctrl_lock);
		if (mfs_helper_openers <= 0)
			return -EPIPE;
		goto retry;
	}

	req = list_first_entry(&mfs_tx_queue, struct mfs_pending_req, tx_node);
	total = sizeof(hdr) + req->payload_len;
	if (count < total) {
		mutex_unlock(&mfs_ctrl_lock);
		return -EMSGSIZE;
	}

	list_del_init(&req->tx_node);
	mutex_unlock(&mfs_ctrl_lock);

	hdr.magic = MFS_CTRL_MAGIC;
	hdr.version = MFS_CTRL_VERSION;
	hdr.req_id = req->req_id;
	hdr.op = req->op;
	hdr.flags = MFS_CTRL_FLAG_REQUEST;
	hdr.status = 0;
	hdr.payload_len = req->payload_len;

	if (copy_to_user(buf, &hdr, sizeof(hdr)))
		return -EFAULT;
	if (req->payload_len && copy_to_user(buf + sizeof(hdr), req->payload, req->payload_len))
		return -EFAULT;

	return total;
}

static ssize_t mfs_ctrl_write(struct file *file, const char __user *buf,
			      size_t count, loff_t *ppos)
{
	struct mfs_ctrl_hdr hdr;
	struct mfs_pending_req *req;
	void *payload = NULL;

	(void)file;
	(void)ppos;

	if (count < sizeof(hdr))
		return -EINVAL;
	if (copy_from_user(&hdr, buf, sizeof(hdr)))
		return -EFAULT;
	if (hdr.magic != MFS_CTRL_MAGIC || hdr.version != MFS_CTRL_VERSION)
		return -EPROTO;
	if (!(hdr.flags & MFS_CTRL_FLAG_RESPONSE))
		return -EPROTO;
	if (hdr.payload_len > MFS_CTRL_MAX_PAYLOAD)
		return -EMSGSIZE;
	if (count != sizeof(hdr) + hdr.payload_len)
		return -EINVAL;

	if (hdr.payload_len) {
		payload = memdup_user(buf + sizeof(hdr), hdr.payload_len);
		if (IS_ERR(payload))
			return PTR_ERR(payload);
	}

	mutex_lock(&mfs_ctrl_lock);
	req = mfs_find_inflight_locked(hdr.req_id);
	if (!req) {
		mutex_unlock(&mfs_ctrl_lock);
		kfree(payload);
		return -ENOENT;
	}

	if (!list_empty(&req->tx_node))
		list_del_init(&req->tx_node);
	list_del_init(&req->inflight_node);
	kfree(req->resp);
	req->resp = payload;
	req->resp_len = hdr.payload_len;
	req->status = hdr.status;
	req->done_flag = true;
	complete_all(&req->done);
	mutex_unlock(&mfs_ctrl_lock);

	return count;
}

static __poll_t mfs_ctrl_poll(struct file *file, poll_table *wait)
{
	__poll_t mask = EPOLLOUT | EPOLLWRNORM;

	(void)file;

	poll_wait(file, &mfs_ctrl_wq, wait);

	mutex_lock(&mfs_ctrl_lock);
	if (!list_empty(&mfs_tx_queue))
		mask |= EPOLLIN | EPOLLRDNORM;
	if (mfs_helper_openers <= 0)
		mask |= EPOLLHUP;
	mutex_unlock(&mfs_ctrl_lock);
	return mask;
}

static int mfs_ctrl_open(struct inode *inode, struct file *file)
{
	(void)inode;
	(void)file;

	mutex_lock(&mfs_ctrl_lock);
	mfs_helper_openers++;
	mutex_unlock(&mfs_ctrl_lock);
	wake_up_interruptible(&mfs_ctrl_wq);
	return 0;
}

static int mfs_ctrl_release(struct inode *inode, struct file *file)
{
	(void)inode;
	(void)file;

	mutex_lock(&mfs_ctrl_lock);
	if (mfs_helper_openers > 0)
		mfs_helper_openers--;
	if (mfs_helper_openers == 0)
		mfs_fail_all_locked(-EPIPE);
	mutex_unlock(&mfs_ctrl_lock);
	wake_up_interruptible(&mfs_ctrl_wq);
	return 0;
}

static const struct file_operations mfs_ctrl_fops = {
	.owner = THIS_MODULE,
	.read = mfs_ctrl_read,
	.write = mfs_ctrl_write,
	.poll = mfs_ctrl_poll,
	.open = mfs_ctrl_open,
	.release = mfs_ctrl_release,
	.llseek = no_llseek,
};

static struct miscdevice mfs_ctrl_miscdev = {
	.minor = MISC_DYNAMIC_MINOR,
	.name = MFS_CTRL_DEV_NAME,
	.fops = &mfs_ctrl_fops,
	.mode = 0600,
};

int mfs_helper_comm_init(void)
{
	atomic_set(&mfs_next_req, 1);
	return misc_register(&mfs_ctrl_miscdev);
}

void mfs_helper_comm_exit(void)
{
	misc_deregister(&mfs_ctrl_miscdev);
	mutex_lock(&mfs_ctrl_lock);
	mfs_fail_all_locked(-ESHUTDOWN);
	mfs_helper_openers = 0;
	mutex_unlock(&mfs_ctrl_lock);
	wake_up_interruptible(&mfs_ctrl_wq);
}
