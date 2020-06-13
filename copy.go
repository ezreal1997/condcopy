package condcopy

import (
	"context"
	"errors"
	"io"
	"time"
)

func CopyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (written int64, err error) {
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

func CopyNWithContext(ctx context.Context, dst io.Writer, src io.Reader, n int64) (written int64, err error) {
	written, err = CopyWithContext(ctx, dst, io.LimitReader(src, n))
	if written == n {
		return n, nil
	}
	if written < n && err == nil {
		// src stopped early; must have been EOF.
		err = io.EOF
	}
	return
}

func CopyWithSleep(sleep <-chan time.Duration, dst io.Writer, src io.Reader) (written int64, err error) {
	buf := make([]byte, 32*1024)
	for {
		select {
		case t := <-sleep:
			time.Sleep(t)
		default:
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

func CopyNWithSleep(sleep <-chan time.Duration, dst io.Writer, src io.Reader, n int64) (written int64, err error) {
	written, err = CopyWithSleep(sleep, dst, io.LimitReader(src, n))
	if written == n {
		return n, nil
	}
	if written < n && err == nil {
		// src stopped early; must have been EOF.
		err = io.EOF
	}
	return
}

func CopyWithTimeout(timeout time.Duration, dst io.Writer, src io.Reader) (written int64, err error) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-t.C:
			return written, errors.New("copy deadline exceeded")
		default:
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

func CopyNWithTimeout(timeout time.Duration, dst io.Writer, src io.Reader, n int64) (written int64, err error) {
	written, err = CopyWithTimeout(timeout, dst, io.LimitReader(src, n))
	if written == n {
		return n, nil
	}
	if written < n && err == nil {
		// src stopped early; must have been EOF.
		err = io.EOF
	}
	return
}
