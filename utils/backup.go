package utils

import (
	"encoding/json"
	"os"

	riak "github.com/basho/riak-go-client"
)

func GetAllBuckets(c *riak.Client) (*riak.ListBucketsResponse, error) {
	bucketList := riak.NewListBucketsCommandBuilder()
	cmd, err := bucketList.Build()
	if err != nil {
		return nil, err
	}
	if err := c.Execute(cmd); err != nil {
		return nil, err
	}
	buckets := cmd.(*riak.ListBucketsCommand).Response
	return buckets, nil
}

func GetBucketProp(c *riak.Client, bucketName string) (*riak.FetchBucketPropsResponse, error) {
	bucketProps := riak.NewFetchBucketPropsCommandBuilder().WithBucket(bucketName)
	cmd, err := bucketProps.Build()
	if err != nil {
		return nil, err
	}
	if err := c.Execute(cmd); err != nil {
		return nil, err
	}
	propert := cmd.(*riak.FetchBucketPropsCommand).Response
	return propert, nil
}

func GetBucketKeys(c *riak.Client, bucketName string) (*riak.ListKeysResponse, error) {
	keyList := riak.NewListKeysCommandBuilder().WithBucket(bucketName)
	cmd, err := keyList.Build()
	if err != nil {
		return nil, err
	}
	if err := c.Execute(cmd); err != nil {
		return nil, err
	}
	keys := cmd.(*riak.ListKeysCommand).Response
	return keys, nil
}

func GetKeyValue(c *riak.Client, bucketName string, key string) (*riak.FetchValueResponse, error) {
	keyValue := riak.NewFetchValueCommandBuilder().WithBucket(bucketName).WithKey(key)
	cmd, err := keyValue.Build()
	if err != nil {
		return nil, err
	}
	if err := c.Execute(cmd); err != nil {
		return nil, err
	}
	values := cmd.(*riak.FetchValueCommand).Response
	return values, nil
}

func WriteToFile(key []*riak.Object, bucket string) {
	fileName := "data/keys/" + bucket + ".json"
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	encoder := json.NewEncoder(file)
	err = encoder.Encode(key)
	if err != nil {
		panic(err)
	}

	err = file.Close()
	if err != nil {
		panic(err)
	}
}
