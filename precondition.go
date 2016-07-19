package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	daapBucketName string
	cdwBucketName  string

	cdwAwsAccessKey string
	cdwAwsSecretKey string

	cdwPrefix  string
	daapPrefix string

	daapRegion string
	cdwRegion  string

	verbose bool
)

func getS3Objects(regionName, bucketName, prefix string, cdw bool) *s3.ListObjectsOutput {
	var s *session.Session

	if cdw {
		s = session.New(&aws.Config{
			Region:      aws.String(regionName),
			Credentials: credentials.NewStaticCredentials(cdwAwsAccessKey, cdwAwsSecretKey, ""),
		})
	} else {
		s = session.New(&aws.Config{
			Region: aws.String(regionName),
		})

	}

	svc := s3.New(s)

	if verbose {
		fmt.Printf("region: %s, bucket: %s, prefix: %s \n", regionName, bucketName, prefix)
	}

	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	// Get the list of all objects
	resp, err := svc.ListObjects(params)
	if err != nil {
		log.Println("Failed to list objects: ", err)
		os.Exit(-1)
	}

	log.Println("Number of objects: ", len(resp.Contents))
	return resp
}

func getLastDateFromDaap() string {

	objects := getS3Objects(daapRegion, daapBucketName, daapPrefix, false)
	for _, key := range objects.Contents {
		// iterate through the list to match the dates range/mso name
		// using the constracted below lookup string

		if verbose {
			log.Println("Key: ", *key.Key)
		}
	}

	return ""
}

func getLastAvailable() string {

	objects := getS3Objects(cdwRegion, cdwBucketName, cdwPrefix, true)
	for _, key := range objects.Contents {
		// iterate through the list to match the dates range/mso name
		// using the constracted below lookup string

		if verbose {
			log.Println("Key: ", *key.Key)
		}
	}
	return ""
}

func init() {
	flagCdwAwsAccessKey := flag.String("K", "", "AWS Access Key for CDW S3")
	flagCdwAwsSecretKey := flag.String("S", "", "AWS Secret Key for CDW S3")

	flagCdwBucketName := flag.String("b", "rovi-cdw", "CDW S3 Bucket name")
	flagDaapBucketName := flag.String("d", "daap-viewership-reports", "CDW S3 Bucket name")

	flagDaapPrefix := flag.String("dp", "cdw-viewership-reports", "Prefix for DaaP S3 bucket")
	flagCdwPrefix := flag.String("cp", "event/tv_viewership", "Prefix for CDW S3 bukcet")

	flagDaapRegion := flag.String("dr", "us-west-2", "Daap S3 Region")
	flagCdwRegion := flag.String("cr", "us-east-1", "CDW S3 Region")

	flagVerbose := flag.Bool("v", false, "Verbose")

	flag.Parse()

	if !flag.Parsed() {
		log.Println("Missing parameters")
		flag.Usage()
		os.Exit(-1)
	}

	cdwAwsAccessKey = *flagCdwAwsAccessKey
	cdwAwsSecretKey = *flagCdwAwsSecretKey

	cdwBucketName = *flagCdwBucketName
	daapBucketName = *flagDaapBucketName

	cdwPrefix = *flagCdwPrefix
	daapPrefix = *flagDaapPrefix

	cdwRegion = *flagCdwRegion
	daapRegion = *flagDaapRegion

	verbose = *flagVerbose
}

func main() {

	if verbose {
		log.Printf("Params provided: -K %s, -S %s, -b %s, -d %s, -v %v\n",
			cdwAwsAccessKey, cdwAwsSecretKey, cdwBucketName, daapBucketName, verbose)
	}
	lastProcessedDate := getLastDateFromDaap()

	maxAvailableDate := getLastAvailable()

	fmt.Print(lastProcessedDate, maxAvailableDate)
}
