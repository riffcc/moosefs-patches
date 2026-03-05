#include <linux/blkdev.h>
#include <linux/cpu.h>
#include <linux/device.h>
#include <linux/errno.h>
#include <linux/kernel.h>
#include <linux/math64.h>
#include <linux/minmax.h>
#include <linux/module.h>
#include <linux/slab.h>

#include "mfsblk.h"

static int mfsblk_major;

static inline struct mfsblk_dev *dev_to_mfsblk(struct device *dev)
{
	return container_of(dev, struct mfsblk_dev, ctrl_dev);
}

static ssize_t master_show(struct device *dev, struct device_attribute *attr,
			   char *buf)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	return scnprintf(buf, PAGE_SIZE, "%s:%u\n", mdev->master_host,
			 mdev->master_port);
}

static ssize_t path_show(struct device *dev, struct device_attribute *attr,
			 char *buf)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	return scnprintf(buf, PAGE_SIZE, "%s\n", mdev->image_path);
}

static ssize_t inode_show(struct device *dev, struct device_attribute *attr,
			  char *buf)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	return scnprintf(buf, PAGE_SIZE, "%u\n", mdev->inode);
}

static ssize_t size_bytes_show(struct device *dev, struct device_attribute *attr,
			       char *buf)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	return scnprintf(buf, PAGE_SIZE, "%llu\n",
			 (unsigned long long)mdev->size_bytes);
}

static ssize_t read_reqs_show(struct device *dev, struct device_attribute *attr,
			      char *buf)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	return scnprintf(buf, PAGE_SIZE, "%lld\n",
			 (long long)atomic64_read(&mdev->stats.read_reqs));
}

static ssize_t write_reqs_show(struct device *dev, struct device_attribute *attr,
			       char *buf)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	return scnprintf(buf, PAGE_SIZE, "%lld\n",
			 (long long)atomic64_read(&mdev->stats.write_reqs));
}

static ssize_t trim_reqs_show(struct device *dev, struct device_attribute *attr,
			      char *buf)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	return scnprintf(buf, PAGE_SIZE, "%lld\n",
			 (long long)atomic64_read(&mdev->stats.trim_reqs));
}

static ssize_t read_bytes_show(struct device *dev, struct device_attribute *attr,
			       char *buf)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	return scnprintf(buf, PAGE_SIZE, "%lld\n",
			 (long long)atomic64_read(&mdev->stats.read_bytes));
}

static ssize_t write_bytes_show(struct device *dev, struct device_attribute *attr,
				char *buf)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	return scnprintf(buf, PAGE_SIZE, "%lld\n",
			 (long long)atomic64_read(&mdev->stats.write_bytes));
}

static ssize_t errors_show(struct device *dev, struct device_attribute *attr,
			   char *buf)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	return scnprintf(buf, PAGE_SIZE, "%lld\n",
			 (long long)atomic64_read(&mdev->stats.errors));
}

static ssize_t inflight_show(struct device *dev, struct device_attribute *attr,
			     char *buf)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	return scnprintf(buf, PAGE_SIZE, "%d\n", atomic_read(&mdev->stats.inflight));
}

static DEVICE_ATTR_RO(master);
static DEVICE_ATTR_RO(path);
static DEVICE_ATTR_RO(inode);
static DEVICE_ATTR_RO(size_bytes);
static DEVICE_ATTR_RO(read_reqs);
static DEVICE_ATTR_RO(write_reqs);
static DEVICE_ATTR_RO(trim_reqs);
static DEVICE_ATTR_RO(read_bytes);
static DEVICE_ATTR_RO(write_bytes);
static DEVICE_ATTR_RO(errors);
static DEVICE_ATTR_RO(inflight);

static struct attribute *mfsblk_dev_attrs[] = {
	&dev_attr_master.attr,
	&dev_attr_path.attr,
	&dev_attr_inode.attr,
	&dev_attr_size_bytes.attr,
	&dev_attr_read_reqs.attr,
	&dev_attr_write_reqs.attr,
	&dev_attr_trim_reqs.attr,
	&dev_attr_read_bytes.attr,
	&dev_attr_write_bytes.attr,
	&dev_attr_errors.attr,
	&dev_attr_inflight.attr,
	NULL,
};

ATTRIBUTE_GROUPS(mfsblk_dev);

static void mfsblk_ctrl_dev_release(struct device *dev)
{
	struct mfsblk_dev *mdev = dev_to_mfsblk(dev);

	complete(&mdev->ctrl_dev_released);
}

static const struct block_device_operations mfsblk_fops = {
	.owner = THIS_MODULE,
};

int mfsblk_dev_module_init(void)
{
	mfsblk_major = register_blkdev(0, MFSBLK_DRV_NAME);
	if (mfsblk_major < 0)
		return mfsblk_major;
	return 0;
}

void mfsblk_dev_module_exit(void)
{
	if (mfsblk_major > 0)
		unregister_blkdev(mfsblk_major, MFSBLK_DRV_NAME);
	mfsblk_major = 0;
}

static int mfsblk_register_ctrl_dev(struct mfsblk_dev *dev)
{
	int ret;

	device_initialize(&dev->ctrl_dev);
	dev->ctrl_dev.bus = &mfsblk_bus_type;
	dev->ctrl_dev.release = mfsblk_ctrl_dev_release;
	dev->ctrl_dev.groups = mfsblk_dev_groups;
	dev_set_name(&dev->ctrl_dev, "%s", dev->name);

	ret = device_add(&dev->ctrl_dev);
	if (ret) {
		put_device(&dev->ctrl_dev);
		wait_for_completion(&dev->ctrl_dev_released);
		return ret;
	}

	dev->ctrl_dev_added = true;
	return 0;
}

static int mfsblk_setup_queue(struct mfsblk_dev *dev)
{
	unsigned int hwq = min_t(unsigned int, num_online_cpus(), 8);
	int ret;

	if (hwq == 0)
		hwq = 1;

	dev->tag_set.ops = &mfsblk_mq_ops;
	dev->tag_set.nr_hw_queues = hwq;
	dev->tag_set.queue_depth = 128;
	dev->tag_set.numa_node = NUMA_NO_NODE;
	dev->tag_set.cmd_size = 0;
	dev->tag_set.flags = BLK_MQ_F_BLOCKING;
	dev->tag_set.driver_data = dev;

	ret = blk_mq_alloc_tag_set(&dev->tag_set);
	if (ret)
		return ret;

	return 0;
}

static int mfsblk_setup_disk(struct mfsblk_dev *dev)
{
	int ret;

	/* Kernel 5.15+: blk_mq_alloc_disk allocates both gendisk and queue */
	dev->disk = blk_mq_alloc_disk(&dev->tag_set, dev);
	if (IS_ERR(dev->disk)) {
		ret = PTR_ERR(dev->disk);
		dev->disk = NULL;
		return ret;
	}
	dev->queue = dev->disk->queue;

	blk_queue_logical_block_size(dev->queue, MFSBLK_SECTOR_SIZE);
	blk_queue_physical_block_size(dev->queue, MFSBLK_BLOCK_SIZE);
	blk_queue_io_min(dev->queue, MFSBLK_BLOCK_SIZE);
	blk_queue_io_opt(dev->queue, MFSBLK_BLOCK_SIZE);
	blk_queue_max_hw_sectors(dev->queue,
				 MFSBLK_CHUNK_SIZE >> MFSBLK_SECTOR_SHIFT);
	blk_queue_max_discard_sectors(dev->queue,
				      MFSBLK_CHUNK_SIZE >> MFSBLK_SECTOR_SHIFT);
	/* blk_queue_discard_granularity() removed in kernel 6.8;
	 * the kernel uses logical block size as default granularity. */
	blk_queue_flag_set(QUEUE_FLAG_NONROT, dev->queue);

	dev->disk->major = mfsblk_major;
	dev->disk->first_minor = dev->id;
	dev->disk->minors = 1;
	dev->disk->fops = &mfsblk_fops;
	dev->disk->private_data = dev;
	strscpy(dev->disk->disk_name, dev->name, sizeof(dev->disk->disk_name));
	set_capacity(dev->disk, div_u64(dev->size_bytes, MFSBLK_SECTOR_SIZE));

	ret = add_disk(dev->disk);
	if (ret) {
		put_disk(dev->disk);
		dev->disk = NULL;
		dev->queue = NULL;
		return ret;
	}

	return 0;
}

int mfsblk_dev_create(const struct mfsblk_map_spec *spec, struct mfsblk_dev **out)
{
	struct mfsblk_dev *dev;
	int ret;

	dev = kzalloc(sizeof(*dev), GFP_KERNEL);
	if (!dev)
		return -ENOMEM;

	INIT_LIST_HEAD(&dev->list);
	init_completion(&dev->ctrl_dev_released);
	mutex_init(&dev->master_lock);
	mutex_init(&dev->cache_lock);
	mutex_init(&dev->conn_lock);
	spin_lock_init(&dev->state_lock);
	atomic_set(&dev->next_msgid, 1);

	dev->id = spec->id;
	dev->master_ip = spec->master_ip;
	dev->master_port = spec->master_port;
	dev->size_bytes = spec->size_bytes;
	dev->inode = spec->inode;
	strscpy(dev->master_host, spec->master_host, sizeof(dev->master_host));
	strscpy(dev->image_path, spec->image_path, sizeof(dev->image_path));
	snprintf(dev->name, sizeof(dev->name), "%s%d", MFSBLK_DISK_PREFIX, dev->id);

	atomic64_set(&dev->stats.read_reqs, 0);
	atomic64_set(&dev->stats.write_reqs, 0);
	atomic64_set(&dev->stats.trim_reqs, 0);
	atomic64_set(&dev->stats.read_bytes, 0);
	atomic64_set(&dev->stats.write_bytes, 0);
	atomic64_set(&dev->stats.errors, 0);
	atomic_set(&dev->stats.inflight, 0);

	mfsblk_cache_init(dev);

	dev->io_wq = alloc_workqueue("mfsblk_io/%d", WQ_UNBOUND | WQ_MEM_RECLAIM,
				    0, dev->id);
	if (!dev->io_wq) {
		ret = -ENOMEM;
		goto err_free;
	}

	ret = mfsblk_setup_queue(dev);
	if (ret)
		goto err_wq;

	ret = mfsblk_setup_disk(dev);
	if (ret)
		goto err_queue;

	ret = mfsblk_register_ctrl_dev(dev);
	if (ret)
		goto err_disk;

	*out = dev;
	return 0;

err_disk:
	del_gendisk(dev->disk);
	put_disk(dev->disk);
	dev->disk = NULL;
err_queue:
	blk_mq_free_tag_set(&dev->tag_set);
	dev->queue = NULL;
err_wq:
	destroy_workqueue(dev->io_wq);
	dev->io_wq = NULL;
	mfsblk_cache_cleanup(dev);
err_free:
	kfree(dev);
	return ret;
}

void mfsblk_dev_destroy(struct mfsblk_dev *dev)
{
	if (!dev)
		return;

	WRITE_ONCE(dev->removing, true);

	if (dev->queue)
		blk_mq_quiesce_queue(dev->queue);
	if (dev->io_wq)
		flush_workqueue(dev->io_wq);

	if (dev->disk) {
		del_gendisk(dev->disk);
		put_disk(dev->disk);  /* frees queue + disk for blk_mq_alloc_disk */
		dev->disk = NULL;
		dev->queue = NULL;
	}
	blk_mq_free_tag_set(&dev->tag_set);

	mfsblk_conn_close_master(dev);
	mfsblk_cache_cleanup(dev);

	if (dev->io_wq) {
		destroy_workqueue(dev->io_wq);
		dev->io_wq = NULL;
	}

	if (dev->ctrl_dev_added) {
		device_unregister(&dev->ctrl_dev);
		wait_for_completion(&dev->ctrl_dev_released);
		dev->ctrl_dev_added = false;
	}

	kfree(dev);
}
