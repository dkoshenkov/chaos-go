package main

import (
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func readProcess() processSample {
	result := processSample{rss: -1, fds: -1}
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) == nil {
		result.cpu = time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
		result.hasCPU = true
	}
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if fields := strings.Fields(line); len(fields) >= 2 && fields[0] == "VmRSS:" {
				if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					result.rss = kb * 1024
				}
				break
			}
		}
	}
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		// /proc/self/fd includes the descriptor opened to enumerate it.
		result.fds = len(entries) - 1
	}
	return result
}
