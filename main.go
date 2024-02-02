package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	riak "github.com/basho/riak-go-client"

	"riak-go-migrator/backup"
)

func main() {
	hostFlag := flag.String("host", "127.0.0.1", "Riak host")
	portFlag := flag.Int("port", 8087, "Riak port")
	backupFlag := flag.Bool("backup", false, "Backup data")
	backupBucketOnly := flag.Bool("bucketonly", false, "Backup only buckets list")
	restoreFlag := flag.Bool("restore", false, "Restore data")
	numWorkers := flag.Int("workers", 1, "Num workers, not more then 256")
	dropKeys := flag.Bool("drop", false, "Drop keys")
	timeout := flag.Int("timeout", 120, "Request timeout in seconds")

	flag.Parse()

	if !*backupFlag && !*restoreFlag && !*dropKeys {
		flag.Usage()
		os.Exit(1)
	}

	var err error

	// un-comment-out to enable debug logging
	// riak.EnableDebugLogging = true

	nodeOpts := &riak.NodeOptions{
		RemoteAddress:  fmt.Sprintf("%s:%d", *hostFlag, *portFlag),
		RequestTimeout: time.Second * time.Duration(*timeout),
	}

	var node *riak.Node

	if node, err = riak.NewNode(nodeOpts); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	nodes := []*riak.Node{node}
	opts := &riak.ClusterOptions{
		Nodes: nodes,
	}

	cluster, err := riak.NewCluster(opts)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	defer func() {
		if err := cluster.Stop(); err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}()

	if err := cluster.Start(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	if *backupFlag {
		log.Println("starting backup...")
		backup.Start(cluster, *backupBucketOnly, *numWorkers)
	}

	if *dropKeys {
		backup.Start(cluster, *backupBucketOnly, *numWorkers)
	}
}
