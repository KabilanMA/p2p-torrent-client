//go:build linux

package p2p

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/KabilanMA/p2p-torrent-client/internal/torrent"
)

// ── io_uring constants (linux/io_uring.h) ────────────────────────────────────

const (
	iouringDepth        = 256        // submission ring depth (power of 2)
	iouringOpWrite      = 23         // IORING_OP_WRITE
	iouringGetEvents    = 1          // IORING_ENTER_GETEVENTS
	iouringOffSQRing    = 0          // IORING_OFF_SQ_RING  mmap offset
	iouringOffCQRing    = 0x8000000  // IORING_OFF_CQ_RING  mmap offset
	iouringOffSQEs      = 0x10000000 // IORING_OFF_SQES     mmap offset
)

// ── kernel ABI structs (must match C structs exactly) ────────────────────────

// iouringParams mirrors struct io_uring_params (120 bytes on x86-64, Linux ≥5.1).
type iouringParams struct {
	sqEntries, cqEntries, flags, sqThreadCPU, sqThreadIdle, features, wqFd uint32
	resv                                                                     [3]uint32
	sqOff                                                                    iouringSQOff
	cqOff                                                                    iouringCQOff
}

// iouringSQOff mirrors struct io_sqring_offsets (40 bytes).
type iouringSQOff struct {
	head, tail, ringMask, ringEntries, flags, dropped, array, resv1 uint32
	userAddr                                                          uint64
}

// iouringCQOff mirrors struct io_cqring_offsets (40 bytes).
type iouringCQOff struct {
	head, tail, ringMask, ringEntries, overflow, cqes, flags, resv1 uint32
	userAddr                                                          uint64
}

// iouringSQE mirrors struct io_uring_sqe (64 bytes).
type iouringSQE struct {
	opcode, flags uint8
	ioprio        uint16
	fd            int32
	off           uint64
	addr          uint64
	length        uint32
	rwFlags       uint32
	userData      uint64
	pad           [24]byte
}

// iouringCQE mirrors struct io_uring_cqe (16 bytes).
type iouringCQE struct {
	userData uint64
	res      int32
	flags    uint32
}

// ── ring ─────────────────────────────────────────────────────────────────────

type iouRing struct {
	fd      int
	sqHead  *uint32
	sqTail  *uint32
	sqMask  *uint32
	sqEnts  uint32 // actual ring capacity
	sqArray []uint32
	sqes    []iouringSQE
	cqHead  *uint32
	cqTail  *uint32
	cqMask  *uint32
	cqes    []iouringCQE
	sqMmap, sqeMmap, cqMmap []byte
}

func newIouRing(depth int) (*iouRing, error) {
	var params iouringParams
	fd, _, errno := unix.Syscall(unix.SYS_IO_URING_SETUP,
		uintptr(depth), uintptr(unsafe.Pointer(&params)), 0)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup: %w", errno)
	}

	r := &iouRing{fd: int(fd)}
	sqRingSize := uintptr(params.sqOff.array) + uintptr(params.sqEntries)*4
	sqeSize := uintptr(params.sqEntries) * 64
	cqRingSize := uintptr(params.cqOff.cqes) + uintptr(params.cqEntries)*16

	var err error
	r.sqMmap, err = unix.Mmap(int(fd), iouringOffSQRing, int(sqRingSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		unix.Close(int(fd))
		return nil, fmt.Errorf("mmap SQ ring: %w", err)
	}
	r.sqeMmap, err = unix.Mmap(int(fd), iouringOffSQEs, int(sqeSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		unix.Munmap(r.sqMmap)
		unix.Close(int(fd))
		return nil, fmt.Errorf("mmap SQEs: %w", err)
	}
	r.cqMmap, err = unix.Mmap(int(fd), iouringOffCQRing, int(cqRingSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		unix.Munmap(r.sqeMmap)
		unix.Munmap(r.sqMmap)
		unix.Close(int(fd))
		return nil, fmt.Errorf("mmap CQ ring: %w", err)
	}

	// Wire up pointers into the mmap'd regions.
	r.sqHead  = (*uint32)(unsafe.Pointer(&r.sqMmap[params.sqOff.head]))
	r.sqTail  = (*uint32)(unsafe.Pointer(&r.sqMmap[params.sqOff.tail]))
	r.sqMask  = (*uint32)(unsafe.Pointer(&r.sqMmap[params.sqOff.ringMask]))
	r.sqEnts  = params.sqEntries
	r.sqArray = unsafe.Slice((*uint32)(unsafe.Pointer(&r.sqMmap[params.sqOff.array])), params.sqEntries)
	r.sqes    = unsafe.Slice((*iouringSQE)(unsafe.Pointer(&r.sqeMmap[0])), params.sqEntries)
	r.cqHead  = (*uint32)(unsafe.Pointer(&r.cqMmap[params.cqOff.head]))
	r.cqTail  = (*uint32)(unsafe.Pointer(&r.cqMmap[params.cqOff.tail]))
	r.cqMask  = (*uint32)(unsafe.Pointer(&r.cqMmap[params.cqOff.ringMask]))
	r.cqes    = unsafe.Slice((*iouringCQE)(unsafe.Pointer(&r.cqMmap[params.cqOff.cqes])), params.cqEntries)

	return r, nil
}

// putSQE appends one WRITE SQE without submitting.
func (r *iouRing) putSQE(fd int32, off uint64, buf []byte, userData uint64) {
	tail := atomic.LoadUint32(r.sqTail)
	idx := tail & *r.sqMask
	sqe := &r.sqes[idx]
	sqe.opcode   = iouringOpWrite
	sqe.flags    = 0
	sqe.ioprio   = 0
	sqe.fd       = fd
	sqe.off      = off
	sqe.addr     = uint64(uintptr(unsafe.Pointer(&buf[0])))
	sqe.length   = uint32(len(buf))
	sqe.rwFlags  = 0
	sqe.userData = userData
	r.sqArray[idx] = idx
	atomic.StoreUint32(r.sqTail, tail+1)
}

// enter submits pending SQEs and optionally waits for minComplete completions.
func (r *iouRing) enter(toSubmit, minComplete uint) error {
	flags := uint(0)
	if minComplete > 0 {
		flags = iouringGetEvents
	}
	_, _, errno := unix.Syscall6(unix.SYS_IO_URING_ENTER,
		uintptr(r.fd), uintptr(toSubmit), uintptr(minComplete), uintptr(flags), 0, 0)
	if errno != 0 {
		return fmt.Errorf("io_uring_enter: %w", errno)
	}
	return nil
}

type cqeResult struct {
	userData uint64
	res      int32
}

// drainCQEs harvests all available CQEs into out. Returns count harvested.
func (r *iouRing) drainCQEs(out []cqeResult) int {
	head := atomic.LoadUint32(r.cqHead)
	tail := atomic.LoadUint32(r.cqTail)
	n := 0
	for head != tail && n < len(out) {
		c := r.cqes[head&*r.cqMask]
		out[n] = cqeResult{c.userData, c.res}
		n++
		head++
	}
	if n > 0 {
		atomic.StoreUint32(r.cqHead, head)
	}
	return n
}

func (r *iouRing) pendingSQEs() uint {
	return uint(atomic.LoadUint32(r.sqTail) - atomic.LoadUint32(r.sqHead))
}

func (r *iouRing) close() {
	unix.Munmap(r.sqMmap)
	unix.Munmap(r.sqeMmap)
	unix.Munmap(r.cqMmap)
	unix.Close(r.fd)
}

// ── uringDiskWriter ──────────────────────────────────────────────────────────

// uringDiskWriter submits piece writes through Linux io_uring, batching
// multiple pieces per syscall to eliminate per-write context-switch overhead.
// Falls back to standard WriteAt if io_uring_setup fails (kernel < 5.1).
type uringDiskWriter struct {
	info     *torrent.TorrentInfo
	files    []*os.File
	fds      []int32 // cached raw file descriptors (stable while files are open)
	offsets  []int64
	totalLen int64
	pieceLen int64
	writeC   chan writeReq
	doneC    chan error
	wg       sync.WaitGroup
}

func newUringDiskWriter(info *torrent.TorrentInfo, output string) (*uringDiskWriter, error) {
	dw := &uringDiskWriter{
		info:     info,
		totalLen: int64(info.TotalLength()),
		pieceLen: int64(info.PieceLength),
		writeC:   make(chan writeReq, 256),
		doneC:    make(chan error, 1),
	}
	if info.IsMultiFile() {
		if err := dw.openMultiFile(info, output); err != nil {
			dw.closeFiles()
			return nil, err
		}
	} else {
		f, err := os.OpenFile(output, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("open output file: %w", err)
		}
		if err := f.Truncate(int64(info.TotalLength())); err != nil {
			f.Close()
			return nil, fmt.Errorf("pre-allocate output file: %w", err)
		}
		dw.files = []*os.File{f}
		dw.offsets = []int64{0}
		dw.fds = []int32{dw.rawFd(f)}
	}
	dw.wg.Add(1)
	go dw.loop()
	return dw, nil
}

func (dw *uringDiskWriter) rawFd(f *os.File) int32 {
	raw, err := f.SyscallConn()
	if err != nil {
		return -1
	}
	var fd int32 = -1
	raw.Control(func(d uintptr) { fd = int32(d) })
	return fd
}

func (dw *uringDiskWriter) openMultiFile(info *torrent.TorrentInfo, outDir string) error {
	var offset int64
	for _, file := range info.Files {
		parts := append([]string{outDir, info.Name}, file.Path...)
		path := filepath.Join(parts...)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
		}
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		if err := f.Truncate(int64(file.Length)); err != nil {
			f.Close()
			return fmt.Errorf("pre-allocate %s: %w", path, err)
		}
		dw.files = append(dw.files, f)
		dw.fds = append(dw.fds, dw.rawFd(f))
		dw.offsets = append(dw.offsets, offset)
		offset += int64(file.Length)
	}
	return nil
}

func (dw *uringDiskWriter) closeFiles() {
	for _, f := range dw.files {
		f.Close()
	}
}

func (dw *uringDiskWriter) Submit(index int, buf []byte) {
	dw.writeC <- writeReq{index: index, buf: buf}
}

func (dw *uringDiskWriter) Close() error {
	close(dw.writeC)
	dw.wg.Wait()
	dw.closeFiles()
	return <-dw.doneC
}

// writeSegment describes one contiguous write within a single file.
type writeSegment struct {
	fd   int32
	off  uint64
	data []byte
}

// segments returns the set of per-file writes needed to store piece[index].
func (dw *uringDiskWriter) segments(index int, data []byte) []writeSegment {
	flatBegin := int64(index) * dw.pieceLen
	flatEnd := flatBegin + int64(len(data))
	var segs []writeSegment
	for i, fd := range dw.fds {
		if fd < 0 {
			continue
		}
		fileStart := dw.offsets[i]
		var fileEnd int64
		if i+1 < len(dw.offsets) {
			fileEnd = dw.offsets[i+1]
		} else {
			fileEnd = dw.totalLen
		}
		if flatEnd <= fileStart || flatBegin >= fileEnd {
			continue
		}
		start := flatBegin
		if fileStart > start {
			start = fileStart
		}
		end := flatEnd
		if fileEnd < end {
			end = fileEnd
		}
		segs = append(segs, writeSegment{
			fd:   fd,
			off:  uint64(start - fileStart),
			data: data[start-flatBegin : end-flatBegin],
		})
	}
	return segs
}

// loop runs the io_uring write loop. Falls back to standard WriteAt if the
// kernel doesn't support io_uring (ENOSYS / EPERM / old kernel).
func (dw *uringDiskWriter) loop() {
	defer dw.wg.Done()

	ring, err := newIouRing(iouringDepth)
	if err != nil {
		dw.loopFallback()
		return
	}
	defer ring.close()

	type pendingPiece struct {
		buf     []byte
		pinner  runtime.Pinner
		segsLeft int32
		hasErr  bool
	}

	inFlight := make(map[uint64]*pendingPiece, iouringDepth)
	cqeBuf := make([]cqeResult, iouringDepth)
	var writeErr error
	var sqPending uint // SQEs queued but not yet submitted via enter()

	flush := func(minWait uint) {
		if sqPending > 0 || minWait > 0 {
			if e := ring.enter(sqPending, minWait); e != nil && writeErr == nil {
				writeErr = e
			}
			sqPending = 0
		}
		n := ring.drainCQEs(cqeBuf)
		for i := 0; i < n; i++ {
			r := cqeBuf[i]
			pp := inFlight[r.userData]
			if pp == nil {
				continue
			}
			if r.res < 0 && writeErr == nil {
				writeErr = fmt.Errorf("io_uring write piece %d: errno %d", r.userData, -r.res)
				pp.hasErr = true
			}
			if atomic.AddInt32(&pp.segsLeft, -1) == 0 {
				pp.pinner.Unpin()
				globalPiecePool.put(pp.buf)
				delete(inFlight, r.userData)
			}
		}
	}

	for req := range dw.writeC {
		segs := dw.segments(req.index, req.buf)
		if len(segs) == 0 {
			globalPiecePool.put(req.buf)
			continue
		}

		// Drain completions if the ring is getting full.
		for uint(len(inFlight))+uint(len(segs)) > uint(ring.sqEnts)/2 {
			flush(1)
		}

		if writeErr != nil {
			// Skip writes on error but still track so Close() can drain.
			globalPiecePool.put(req.buf)
			continue
		}

		pp := &pendingPiece{buf: req.buf, segsLeft: int32(len(segs))}
		pp.pinner.Pin(&req.buf[0])
		inFlight[uint64(req.index)] = pp

		for _, seg := range segs {
			ring.putSQE(seg.fd, seg.off, seg.data, uint64(req.index))
			sqPending++
		}
	}

	// Channel closed: submit remaining and wait for all completions.
	if sqPending > 0 {
		ring.enter(sqPending, 0)
		sqPending = 0
	}
	for len(inFlight) > 0 {
		flush(1)
	}

	dw.doneC <- writeErr
}

// loopFallback is the standard WriteAt path used when io_uring is unavailable.
func (dw *uringDiskWriter) loopFallback() {
	var writeErr error
	for req := range dw.writeC {
		if writeErr == nil {
			if err := dw.writePiece(req.index, req.buf); err != nil {
				writeErr = err
			}
		}
		globalPiecePool.put(req.buf)
	}
	dw.doneC <- writeErr
}

func (dw *uringDiskWriter) writePiece(index int, data []byte) error {
	flatBegin := int64(index) * dw.pieceLen
	flatEnd := flatBegin + int64(len(data))
	for i, f := range dw.files {
		fileStart := dw.offsets[i]
		var fileEnd int64
		if i+1 < len(dw.offsets) {
			fileEnd = dw.offsets[i+1]
		} else {
			fileEnd = dw.totalLen
		}
		if flatEnd <= fileStart || flatBegin >= fileEnd {
			continue
		}
		start := flatBegin
		if fileStart > start {
			start = fileStart
		}
		end := flatEnd
		if fileEnd < end {
			end = fileEnd
		}
		chunk := data[start-flatBegin : end-flatBegin]
		if _, err := f.WriteAt(chunk, start-fileStart); err != nil {
			return fmt.Errorf("WriteAt %s +%d: %w", f.Name(), start-fileStart, err)
		}
	}
	return nil
}
