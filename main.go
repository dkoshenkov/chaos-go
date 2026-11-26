package main

import (
	"os"

	"github.com/dkoshenkov/chaos-go/internal/server"
)

func main() {
	os.Exit(server.Main(os.Args[1:], os.Stderr))
}
