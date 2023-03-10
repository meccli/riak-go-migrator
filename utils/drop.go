package utils

import (
	riak "github.com/basho/riak-go-client"
)

func DropKey(c *riak.Client, bucketName string, key string) error {
	keyValue := riak.NewDeleteValueCommandBuilder().WithBucket(bucketName).WithKey(key)
	cmd, err := keyValue.Build()
	if err != nil {
		return err
	}
	if err := c.Execute(cmd); err != nil {
		return err
	}
	return nil
}
