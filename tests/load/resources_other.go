//go:build !linux

package main

func readProcess() processSample {
	return processSample{rss: -1, fds: -1}
}
