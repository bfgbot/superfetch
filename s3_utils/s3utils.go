package s3_utils

import (
	"context"
	"io"
	"log"
	"os"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ParseS3Uri takes an S3 URI string and returns the bucket and key components.
// It expects the URI to be in the format "s3://<bucket>/<key>".
// If the URI format is invalid, it panics with an error message.
func ParseS3Uri(uri string) (bucket, key string) {
	re := regexp.MustCompile(`^s3://([^/]+)/(.*)$`)
	matches := re.FindStringSubmatch(uri)
	if matches == nil {
		log.Panicln("Invalid S3 URI format. Must be s3://<bucket>/...", uri)
	}
	bucket = matches[1]
	key = matches[2]
	return
}

var s3Client *s3.Client

// getS3Client returns an S3 client using the default AWS configuration.
// It uses credentials from either the .aws config file or IAM role
// associated with the EC2 instance where this code is running.
func getS3Client() *s3.Client {
	if s3Client != nil {
		return s3Client
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithEC2IMDSRegion())
	if err != nil {
		log.Panicln("Failed to load AWS config:", err)
	}

	s3Client = s3.NewFromConfig(cfg)
	return s3Client
}

// UploadFile uploads a local file to an S3 bucket.
func UploadFile(localFile, bucket, key string) {
	fp, err := os.Open(localFile)
	if err != nil {
		log.Panicln("Failed to open file:", localFile, err)
	}
	defer fp.Close()

	client := getS3Client()
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   fp,
	})
	if err != nil {
		log.Panicln("Failed to upload to S3:", bucket, key, err)
	}
}

// DownloadFile downloads a file from an S3 bucket and returns the local file path.
func DownloadFile(bucket, key string) string {
	fp, err := os.CreateTemp("", "")
	if err != nil {
		log.Panicln("Failed to create temp file:", err)
	}
	defer fp.Close()

	client := getS3Client()
	res, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Panicln("Failed to initiate S3 download:", bucket, key, err)
	}
	defer res.Body.Close()

	_, err = io.Copy(fp, res.Body)
	if err != nil {
		log.Panicln("Failed to download from S3:", bucket, key, err)
	}

	return fp.Name()
}
