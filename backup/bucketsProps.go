package backup

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	riak "github.com/basho/riak-go-client"

	"riak-go-migrator/utils"
)

func backupBucketProps(c *riak.Cluster, buckets *riak.ListBucketsResponse) {
	log.Printf("Got %d buckets", len(buckets.Buckets))
	log.Println("Processing of buckets...")
	bucketsProps, err := os.Create("bucketsProps.json")
	if err != nil {
		panic(err)
	}
	defer bucketsProps.Close()

	var bucketList []utils.BucketInfo

	for _, bucket := range buckets.Buckets {
		props, err := utils.GetBucketProp(c, bucket)
		if err != nil {
			panic(err)
		}
		b, err := json.MarshalIndent(props, "", "  ")
		if err != nil {
			fmt.Println(err)
		}
		bucketInfo := utils.BucketInfo{Name: bucket, Props: string(b)}
		bucketList = append(bucketList, bucketInfo)
	}

	b, err := json.MarshalIndent(bucketList, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(bucketsProps, string(b))
	log.Println("Processing of buckets completed.")
}
