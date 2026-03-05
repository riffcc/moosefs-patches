#include <linux/ctype.h>
#include <linux/errno.h>
#include <linux/idr.h>
#include <linux/in.h>
#include <linux/inet.h>
#include <linux/jhash.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/overflow.h>
#include <linux/slab.h>
#include <linux/string.h>

#include "mfsblk.h"

struct bus_type mfsblk_bus_type = {
	.name = MFSBLK_DRV_NAME,
};

static DEFINE_MUTEX(mfsblk_devices_lock);
static LIST_HEAD(mfsblk_devices);
static DEFINE_IDA(mfsblk_ids);

static int mfsblk_parse_ipv4(const char *host, u32 *ip)
{
	u8 bytes[4];

	if (!in4_pton(host, -1, bytes, -1, NULL))
		return -EINVAL;

	*ip = ((u32)bytes[0] << 24) |
	      ((u32)bytes[1] << 16) |
	      ((u32)bytes[2] << 8) |
	      (u32)bytes[3];
	return 0;
}

static int mfsblk_parse_size(const char *value, u64 *bytes)
{
	u64 base;
	u64 mult = 1;
	size_t n = strlen(value);
	char *end;
	char unit;
	int ret;

	if (!n)
		return -EINVAL;

	unit = value[n - 1];
	if (isalpha(unit)) {
		switch (toupper(unit)) {
		case 'K':
			mult = 1024ULL;
			break;
		case 'M':
			mult = 1024ULL * 1024ULL;
			break;
		case 'G':
			mult = 1024ULL * 1024ULL * 1024ULL;
			break;
		case 'T':
			mult = 1024ULL * 1024ULL * 1024ULL * 1024ULL;
			break;
		default:
			return -EINVAL;
		}
	}

	base = simple_strtoull(value, &end, 10);
	if (end == value)
		return -EINVAL;
	if (*end && toupper(*end) != unit)
		return -EINVAL;

	ret = check_mul_overflow(base, mult, bytes);
	if (ret)
		return -ERANGE;
	if (*bytes == 0)
		return -EINVAL;

	return 0;
}

static int mfsblk_parse_master(const char *value, struct mfsblk_map_spec *spec)
{
	char *tmp;
	char *host;
	char *port_str;
	unsigned int port = MFSBLK_DEFAULT_MASTER_PORT;
	int ret;

	tmp = kstrdup(value, GFP_KERNEL);
	if (!tmp)
		return -ENOMEM;

	host = tmp;
	port_str = strchr(tmp, ':');
	if (port_str) {
		*port_str = '\0';
		port_str++;
		ret = kstrtouint(port_str, 10, &port);
		if (ret || port > U16_MAX) {
			kfree(tmp);
			return -EINVAL;
		}
	}

	ret = mfsblk_parse_ipv4(host, &spec->master_ip);
	if (ret) {
		kfree(tmp);
		return ret;
	}

	strscpy(spec->master_host, host, sizeof(spec->master_host));
	spec->master_port = (u16)port;
	kfree(tmp);
	return 0;
}

static int mfsblk_parse_spec(char *spec_str, struct mfsblk_map_spec *spec)
{
	char *cursor = spec_str;
	char *item;
	bool have_master = false;
	bool have_path = false;
	bool have_size = false;
	int ret;

	memset(spec, 0, sizeof(*spec));
	spec->master_port = MFSBLK_DEFAULT_MASTER_PORT;

	while ((item = strsep(&cursor, ",")) != NULL) {
		char *key;
		char *value;

		item = strim(item);
		if (!*item)
			continue;

		key = item;
		value = strchr(item, '=');
		if (!value)
			return -EINVAL;
		*value = '\0';
		value = strim(value + 1);
		key = strim(key);

		if (!strcmp(key, "master")) {
			ret = mfsblk_parse_master(value, spec);
			if (ret)
				return ret;
			have_master = true;
		} else if (!strcmp(key, "path")) {
			if (!*value)
				return -EINVAL;
			strscpy(spec->image_path, value, sizeof(spec->image_path));
			have_path = true;
		} else if (!strcmp(key, "size")) {
			ret = mfsblk_parse_size(value, &spec->size_bytes);
			if (ret)
				return ret;
			have_size = true;
		} else if (!strcmp(key, "inode")) {
			ret = kstrtou32(value, 10, &spec->inode);
			if (ret)
				return ret;
			spec->inode_explicit = true;
		} else {
			return -EINVAL;
		}
	}

	if (!have_master || !have_path || !have_size)
		return -EINVAL;

	if (!spec->inode_explicit)
		spec->inode = jhash(spec->image_path, strlen(spec->image_path), 0x4d465342U);

	return 0;
}

static int mfsblk_add_one(char *spec_str)
{
	struct mfsblk_map_spec spec;
	struct mfsblk_dev *dev;
	int ret;

	ret = mfsblk_parse_spec(spec_str, &spec);
	if (ret)
		return ret;

	ret = ida_alloc(&mfsblk_ids, GFP_KERNEL);
	if (ret < 0)
		return ret;
	spec.id = ret;

	ret = mfsblk_dev_create(&spec, &dev);
	if (ret) {
		ida_free(&mfsblk_ids, spec.id);
		return ret;
	}

	mutex_lock(&mfsblk_devices_lock);
	list_add_tail(&dev->list, &mfsblk_devices);
	mutex_unlock(&mfsblk_devices_lock);
	return 0;
}

static int mfsblk_remove_one(const char *name)
{
	struct mfsblk_dev *dev;
	struct mfsblk_dev *found = NULL;

	mutex_lock(&mfsblk_devices_lock);
	list_for_each_entry(dev, &mfsblk_devices, list) {
		if (!strcmp(dev->name, name)) {
			list_del(&dev->list);
			found = dev;
			break;
		}
	}
	mutex_unlock(&mfsblk_devices_lock);

	if (!found)
		return -ENOENT;

	ida_free(&mfsblk_ids, found->id);
	mfsblk_dev_destroy(found);
	return 0;
}

static ssize_t add_store(const struct bus_type *bus, const char *buf, size_t count)
{
	char *spec;
	int ret;

	(void)bus;

	spec = kstrndup(buf, count, GFP_KERNEL);
	if (!spec)
		return -ENOMEM;
	strim(spec);

	ret = mfsblk_add_one(spec);
	kfree(spec);
	if (ret)
		return ret;

	return count;
}

static ssize_t remove_store(const struct bus_type *bus, const char *buf, size_t count)
{
	char *name;
	int ret;

	(void)bus;

	name = kstrndup(buf, count, GFP_KERNEL);
	if (!name)
		return -ENOMEM;
	strim(name);

	ret = mfsblk_remove_one(name);
	kfree(name);
	if (ret)
		return ret;

	return count;
}

static BUS_ATTR_WO(add);
static BUS_ATTR_WO(remove);

static int __init mfsblk_init(void)
{
	int ret;

	ret = mfsblk_dev_module_init();
	if (ret)
		return ret;

	ret = bus_register(&mfsblk_bus_type);
	if (ret)
		goto err_blk;

	ret = bus_create_file(&mfsblk_bus_type, &bus_attr_add);
	if (ret)
		goto err_bus;

	ret = bus_create_file(&mfsblk_bus_type, &bus_attr_remove);
	if (ret)
		goto err_add;

	pr_info("mfsblk: loaded\n");
	return 0;

err_add:
	bus_remove_file(&mfsblk_bus_type, &bus_attr_add);
err_bus:
	bus_unregister(&mfsblk_bus_type);
err_blk:
	mfsblk_dev_module_exit();
	return ret;
}

static void __exit mfsblk_exit(void)
{
	struct mfsblk_dev *dev;
	struct mfsblk_dev *tmp;

	bus_remove_file(&mfsblk_bus_type, &bus_attr_remove);
	bus_remove_file(&mfsblk_bus_type, &bus_attr_add);

	mutex_lock(&mfsblk_devices_lock);
	list_for_each_entry_safe(dev, tmp, &mfsblk_devices, list) {
		list_del(&dev->list);
		ida_free(&mfsblk_ids, dev->id);
		mutex_unlock(&mfsblk_devices_lock);
		mfsblk_dev_destroy(dev);
		mutex_lock(&mfsblk_devices_lock);
	}
	mutex_unlock(&mfsblk_devices_lock);

	ida_destroy(&mfsblk_ids);
	bus_unregister(&mfsblk_bus_type);
	mfsblk_dev_module_exit();
	pr_info("mfsblk: unloaded\n");
}

module_init(mfsblk_init);
module_exit(mfsblk_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Codex");
MODULE_DESCRIPTION("MooseFS block device kernel module");
