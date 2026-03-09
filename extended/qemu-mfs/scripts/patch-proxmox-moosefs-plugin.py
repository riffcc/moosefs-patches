#!/usr/bin/env python3
from pathlib import Path
import sys


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f"missing expected plugin block: {label}")
    return text.replace(old, new, 1)


plugin_path = Path(sys.argv[1] if len(sys.argv) > 1 else "/usr/share/perl5/PVE/Storage/Custom/MooseFSPlugin.pm")
text = plugin_path.read_text()

if "mfsqemu" in text and "_mfs_master_endpoint" in text:
    print(f"{plugin_path} already patched for qemu-mfs")
    raise SystemExit(0)

text = replace_once(
    text,
    "use Data::Dumper qw(Dumper);\n",
    "use Data::Dumper qw(Dumper);\nuse Digest::MD5 qw(md5_hex);\n",
    "Digest::MD5 import",
)

text = replace_once(
    text,
    """        mfsbdev => {\n            description => "Enable mfsbdev (NBD) acceleration for raw VM images. Non-raw formats and LXC containers automatically use FUSE. One storage serves both VMs and containers.",\n            type => 'boolean',\n        },\n""",
    """        mfsbdev => {\n            description => "Enable mfsbdev (NBD) acceleration for raw VM images. Non-raw formats and LXC containers automatically use FUSE. One storage serves both VMs and containers.",\n            type => 'boolean',\n        },\n        mfsqemu => {\n            description => "Enable direct qemu-mfs access for VM disks. QEMU talks to MooseFS directly instead of using FUSE or mfsbdev for guest block I/O.",\n            type => 'boolean',\n        },\n""",
    "properties mfsqemu",
)

text = replace_once(
    text,
    """        mfsbdev => { optional => 1 },\n        mfsnbdlink => { optional => 1, advanced => 1 },\n""",
    """        mfsbdev => { optional => 1 },\n        mfsqemu => { optional => 1 },\n        mfsnbdlink => { optional => 1, advanced => 1 },\n""",
    "options mfsqemu",
)

text = replace_once(
    text,
    "    # bdev not configured at storage level\n    return 0 unless $scfg->{mfsbdev};\n",
    "    return 0 if $scfg->{mfsqemu};\n\n    # bdev not configured at storage level\n    return 0 unless $scfg->{mfsbdev};\n",
    "_should_use_bdev mfsqemu guard",
)

text = replace_once(
    text,
    """sub get_volume_attribute {\n    return PVE::Storage::DirPlugin::get_volume_attribute(@_);\n}\n""",
    """sub _mfs_master_endpoint {\n    my ($class, $scfg) = @_;\n\n    my $master = $scfg->{mfsmaster} // 'mfsmaster';\n    my $port = $scfg->{mfsport} // '9421';\n    return \"$master:$port\";\n}\n\nsub _mfs_volume_path {\n    my ($class, $scfg, $volname) = @_;\n\n    my $fs_path = $class->filesystem_path($scfg, $volname);\n    my $base = $scfg->{path} // die \"storage path is undefined\\n\";\n    die \"volume path '$fs_path' is not under storage base '$base'\\n\"\n        if index($fs_path, $base) != 0;\n\n    my $rel = substr($fs_path, length($base));\n    $rel =~ s{^/+}{};\n\n    my $subfolder = $scfg->{mfssubfolder} // '';\n    $subfolder =~ s{^/+}{};\n    $subfolder =~ s{/+$}{};\n\n    return $subfolder ne '' ? \"/$subfolder/$rel\" : \"/$rel\";\n}\n\nsub get_volume_attribute {\n    return PVE::Storage::DirPlugin::get_volume_attribute(@_);\n}\n""",
    "helper functions",
)

text = replace_once(
    text,
    """sub qemu_blockdev_options {\n    my ($class, $scfg, $storeid, $volname, $machine_version, $options) = @_;\n\n    # For bdev-eligible volumes, try to return host_device blockdev for the NBD device\n    if ($class->_should_use_bdev($scfg, $volname)) {\n        my ($path) = $class->path($scfg, $volname, $storeid);\n        if ($path && $path =~ m|^/dev/nbd\\d+|) {\n            log_debug \"[qemu_blockdev_options] Using host_device driver for NBD: $path\";\n            return {\n                driver => 'host_device',\n                filename => $path,\n            };\n        }\n    }\n\n    # For FUSE volumes (qcow2, vmdk, LXC, etc.), use parent implementation\n    return $class->SUPER::qemu_blockdev_options($scfg, $storeid, $volname, $machine_version, $options);\n}\n""",
    """sub qemu_blockdev_options {\n    my ($class, $scfg, $storeid, $volname, $machine_version, $options) = @_;\n\n    if ($scfg->{mfsqemu}) {\n        my $mfs_path = $class->_mfs_volume_path($scfg, $volname);\n        my $blockdev = {\n            driver => 'mfs',\n            master => $class->_mfs_master_endpoint($scfg),\n            path => $mfs_path,\n        };\n\n        if (defined($scfg->{mfspassword}) && $scfg->{mfspassword} ne '') {\n            $blockdev->{'password-md5'} = md5_hex($scfg->{mfspassword});\n        }\n\n        log_debug \"[qemu_blockdev_options] Using qemu-mfs driver for $volname => $mfs_path\";\n        return $blockdev;\n    }\n\n    # For bdev-eligible volumes, try to return host_device blockdev for the NBD device\n    if ($class->_should_use_bdev($scfg, $volname)) {\n        my ($path) = $class->path($scfg, $volname, $storeid);\n        if ($path && $path =~ m|^/dev/nbd\\d+|) {\n            log_debug \"[qemu_blockdev_options] Using host_device driver for NBD: $path\";\n            return {\n                driver => 'host_device',\n                filename => $path,\n            };\n        }\n    }\n\n    # For FUSE volumes (qcow2, vmdk, LXC, etc.), use parent implementation\n    return $class->SUPER::qemu_blockdev_options($scfg, $storeid, $volname, $machine_version, $options);\n}\n""",
    "qemu_blockdev_options direct driver",
)

text = replace_once(
    text,
    "    if ($scfg->{mfsbdev} && !moosefs_bdev_is_active($scfg)) {\n",
    "    if ($scfg->{mfsbdev} && !$scfg->{mfsqemu} && !moosefs_bdev_is_active($scfg)) {\n",
    "activate_storage mfsqemu guard",
)

backup = plugin_path.with_suffix(plugin_path.suffix + ".qemu-mfs.bak")
if not backup.exists():
    backup.write_text(plugin_path.read_text())

plugin_path.write_text(text)
print(f"patched {plugin_path}")
