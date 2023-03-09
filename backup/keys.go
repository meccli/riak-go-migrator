package backup

import (
	"log"

	riak "github.com/basho/riak-go-client"

	"riak-go-migrator/utils"
)

func backupBucketKeys(c *riak.Client, buckets *riak.ListBucketsResponse) {
	for _, bucket := range buckets.Buckets {
		log.Printf("Processing bucket: %s\n", bucket)

		keys, err := utils.GetBucketKeys(c, bucket)
		if err != nil {
			panic(err)
		}

		for _, key := range keys.Keys {
			log.Printf("Processing key: %s\n", key)

			value, err := utils.GetKeyValue(c, bucket, key)
			if err != nil {
				panic(err)
			}

			utils.WriteToFile(value.Values, bucket)
		}

		log.Printf("Processed data for bucket %s\n", bucket)
	}
}
