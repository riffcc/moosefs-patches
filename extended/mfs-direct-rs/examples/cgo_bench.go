package main

/*
#cgo CFLAGS: -I../include
#cgo LDFLAGS: -L../target/release -lmoosefs_direct -Wl,-rpath,../target/release
#include "moosefs_direct.h"
#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"time"
	"unsafe"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "usage: %s <master:port> <path> <size-bytes>\n", os.Args[0])
		os.Exit(2)
	}

	master := os.Args[1]
	path := os.Args[2]
	size := parseSize(os.Args[3])
	password := os.Getenv("MFS_PASSWORD")
	if password == "" {
		fmt.Fprintln(os.Stderr, "MFS_PASSWORD must be set")
		os.Exit(2)
	}

	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		fmt.Fprintf(os.Stderr, "failed to fill payload: %v\n", err)
		os.Exit(1)
	}

	cMaster := C.CString(master)
	cPath := C.CString(path)
	cPassword := C.CString(password)
	cSubdir := C.CString("/")
	defer C.free(unsafe.Pointer(cMaster))
	defer C.free(unsafe.Pointer(cPath))
	defer C.free(unsafe.Pointer(cPassword))
	defer C.free(unsafe.Pointer(cSubdir))

	connectStarted := time.Now()
	handle := C.mfs_client_connect(cMaster, cSubdir, cPassword)
	if handle == nil {
		fmt.Fprintln(os.Stderr, "mfs_client_connect failed")
		os.Exit(1)
	}
	defer C.mfs_client_destroy(handle)
	connectElapsed := time.Since(connectStarted)

	writeStarted := time.Now()
	rc := C.mfs_client_write_all(
		handle,
		cPath,
		(*C.uchar)(unsafe.Pointer(unsafe.SliceData(payload))),
		C.size_t(len(payload)),
	)
	if rc != 0 {
		failWithClientError(handle, "mfs_client_write_all")
	}
	writeElapsed := time.Since(writeStarted)

	var outPtr *C.uchar
	var outLen C.size_t
	readStarted := time.Now()
	rc = C.mfs_client_read_all(handle, cPath, &outPtr, &outLen)
	if rc != 0 {
		failWithClientError(handle, "mfs_client_read_all")
	}
	readElapsed := time.Since(readStarted)
	defer C.mfs_client_free_buffer(outPtr, outLen)

	readBack := unsafe.Slice((*byte)(unsafe.Pointer(outPtr)), int(outLen))
	if !bytes.Equal(readBack, payload) {
		fmt.Fprintln(os.Stderr, "verification failed")
		os.Exit(1)
	}

	fmt.Printf(
		"cgo_bench master=%s path=%s size=%d connect_ms=%.3f write_ms=%.3f write_mib_s=%.2f read_ms=%.3f read_mib_s=%.2f\n",
		master,
		path,
		size,
		millis(connectElapsed),
		millis(writeElapsed),
		mibPerSec(size, writeElapsed),
		millis(readElapsed),
		mibPerSec(size, readElapsed),
	)
}

func failWithClientError(handle *C.MfsClientHandle, op string) {
	msg := C.mfs_client_last_error(handle)
	if msg == nil {
		fmt.Fprintf(os.Stderr, "%s failed with unknown error\n", op)
	} else {
		fmt.Fprintf(os.Stderr, "%s failed: %s\n", op, C.GoString(msg))
	}
	os.Exit(1)
}

func parseSize(raw string) int {
	var size int
	if _, err := fmt.Sscanf(raw, "%d", &size); err != nil || size <= 0 {
		fmt.Fprintf(os.Stderr, "invalid size-bytes: %s\n", raw)
		os.Exit(2)
	}
	return size
}

func millis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func mibPerSec(size int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return (float64(size) / (1024.0 * 1024.0)) / d.Seconds()
}
