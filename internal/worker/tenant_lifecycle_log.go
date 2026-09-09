package worker

import (
	"fmt"
	"os"
	"sync"
)

type tenantProcessLog struct {
	mu        sync.Mutex
	file      *os.File
	remaining int64
	err       error
}

func (l *tenantProcessLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if int64(len(p)) > l.remaining {
		l.err = fmt.Errorf("process log exceeded 64 MiB bound; run incomplete")
		return 0, l.err
	}
	n, err := l.file.Write(p)
	l.remaining -= int64(n)
	if err != nil {
		l.err = err
	}
	return n, err
}
