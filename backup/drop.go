package backup

import (
	"log"

	riak "github.com/basho/riak-go-client"

	"riak-go-migrator/utils"
)

func dropKeys(c *riak.Client, buckets *riak.ListBucketsResponse, numWorkers int) {
	for _, bucket := range buckets.Buckets {
		log.Printf("Processing bucket: %s\n", bucket)

		keys, err := utils.GetBucketKeys(c, bucket)
		if err != nil {
			panic(err)
		}

		keyChan := make(chan string)

		for i := 0; i < numWorkers; i++ {
			go func() {
				for key := range keyChan {
					log.Printf("Processing key: %s\n", key)

					err := utils.DropKey(c, bucket, key)
					if err != nil {
						panic(err)
					}
				}
			}()
		}

		for _, key := range keys.Keys {
			keyChan <- key
		}

		log.Printf("Processed data for bucket %s\n", bucket)
	}
}
