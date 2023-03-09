package main

import (
	"flag"
	"fmt"
	"os"

	riak "github.com/basho/riak-go-client"
	util "github.com/basho/taste-of-riak/go/util"

	"riak-go-migrator/backup"
)

func main() {
	hostFlag := flag.String("host", "127.0.0.1", "Riak host")
	portFlag := flag.Int("port", 8087, "Riak port")
	backupFlag := flag.Bool("backup", false, "Backup data")
	restoreFlag := flag.Bool("restore", false, "Restore data")
	numWorkers := flag.Int("workers", 1, "Num workers")

	flag.Parse()

	if !*backupFlag && !*restoreFlag {
		flag.Usage()
		os.Exit(1)
	}

	var err error

	// un-comment-out to enable debug logging
	// riak.EnableDebugLogging = true

	o := &riak.NewClientOptions{
		RemoteAddresses: []string{fmt.Sprintf("%s:%d", *hostFlag, *portFlag)},
	}

	var c *riak.Client
	c, err = riak.NewClient(o)
	if err != nil {
		util.ErrExit(err)
	}

	defer func() {
		if err := c.Stop(); err != nil {
			util.ErrExit(err)
		}
	}()

	if *backupFlag {
		backup.Start(c, *numWorkers)
	}
}
