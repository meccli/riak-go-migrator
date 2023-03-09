package backup

import (
	"riak-go-migrator/utils"

	riak "github.com/basho/riak-go-client"
)

func Start(c *riak.Client, numWorkers int) {
	buckets, err := utils.GetAllBuckets(c)
	if err != nil {
		panic(err)
	}
	backupBucketProps(c, buckets)
	backupBucketKeys(c, buckets, numWorkers)
}
