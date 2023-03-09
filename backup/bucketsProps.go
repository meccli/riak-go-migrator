package backup

import (
	"encoding/json"
	"fmt"
	"os"

	riak "github.com/basho/riak-go-client"

	"riak-go-migrator/utils"
)

func backupBucketProps(c *riak.Client, buckets *riak.ListBucketsResponse) {
	bucketsProps, err := os.Create("data/bucketsProps.json")
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
}
