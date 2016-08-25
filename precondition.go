package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

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

	verbose         bool
	msoListFilename string
	daapOnly        bool
	exactMsos       bool

	msoLookup map[string]string
	msoList   []msoType
)

func init() {
	flagCdwAwsAccessKey := flag.String("K", "", "AWS Access Key for CDW S3")
	flagCdwAwsSecretKey := flag.String("S", "", "AWS Secret Key for CDW S3")

	flagCdwBucketName := flag.String("b", "rovi-cdw", "CDW S3 Bucket name")
	flagDaapBucketName := flag.String("d", "daaprawcdwdata", "CDW S3 Bucket name")

	flagDaapPrefix := flag.String("dp", "hh_count2d", "Prefix for DaaP S3 hh count bucket")
	flagCdwPrefix := flag.String("cp", "event/tv_viewership", "Prefix for CDW S3 bukcet")

	flagDaapRegion := flag.String("dr", "us-west-2", "Daap S3 Region")
	flagCdwRegion := flag.String("cr", "us-east-1", "CDW S3 Region")

	flagMsoFileName := flag.String("m", "mso-list.csv", "Filename for `MSO` list")
	flagVerbose := flag.Bool("v", false, "Verbose")

	flagDaapOnly := flag.Bool("D", false, "Check only DaaP reports date")
	flagExactMso := flag.Bool("E", false, "Check only DaaP reports date for exact MSO-s")

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

	msoListFilename = *flagMsoFileName
	verbose = *flagVerbose
	daapOnly = *flagDaapOnly
	exactMsos = *flagExactMso

	msoList, msoLookup = getMsoNamesList()
}

type msoType struct {
	Code string
	Name string
}

// getMsoNamesList reads the list of MSO's and initializes the mso lookup map and array
func getMsoNamesList() ([]msoType, map[string]string) {
	msoList := []msoType{}
	msoLookup := make(map[string]string)

	msoFile, err := os.Open(msoListFilename)
	if err != nil {
		log.Fatalf("Could not open Mso List file: %s, Error: %s\n", msoListFilename, err)
	}

	r := csv.NewReader(msoFile)
	r.TrimLeadingSpace = true

	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("Could not read MSO file: %s, Error: %s\n", msoListFilename, err)
	}

	for _, record := range records {
		msoList = append(msoList, msoType{record[0], record[1]})
		msoLookup[record[0]] = record[1]
	}
	return msoList, msoLookup
}

// getS3Objects retrives the list of objects from AWS S3
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
		log.Printf("region: %s, bucket: %s, prefix: %s \n", regionName, bucketName, prefix)
		log.Println(msoList)
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

// gotToFar checks if we got to Jan 01, 2016
func gotToFar(date time.Time) bool {
	yy, mm, dd := date.Date()

	if mm == 1 && yy == 2016 && dd == 1 {
		return true
	}

	return false
}

func buildDatePrefix(date time.Time) string {
	yy, mm, dd := date.Date()

	return fmt.Sprintf("%04d%02d%02d", yy, mm, dd)
}

func formatOutputDate(date time.Time) string {
	year, month, day := date.Date()

	return fmt.Sprintf("%4d-%02d-%02d", year, int(month), day)
}

// getDatesForAggregates looks up when the last aggregated report date and last normal per-MSO viewership report date
func getDatesForAggregates() (bool, string) {
	lastAggregatedDate := "None"
	lastDate := ""

	found := false

	date := time.Now()
	// Starting from today
	for {
		lastDate = buildDatePrefix(date)
		if verbose {
			log.Println("Prefix: ", daapPrefix+"/"+lastDate)

		}
		objects := getS3Objects(daapRegion, daapBucketName, daapPrefix+"/"+lastDate, false)

		// is there are reports for the number of MSO-s?
		if len(objects.Contents) != len(msoList) {
			date = date.AddDate(0, 0, -1)
			if gotToFar(date) {
				break
			}
			continue
		}

		// we have the date when there are N reports for MSO's aggregated
		// But report one day after - to start FROM
		found = true
		lastAggregatedDate = formatOutputDate(date)
		break
	}

	return found, lastAggregatedDate
}

// getLastDateFromDaap looks up when was the last successfull run of Daap
func getLastDateFromDaap() (bool, string, string) {
	// offset is for aggregated report count = len(mso-list)+1 (aggregated report)
	lastDateAnyReport := "None"
	lastDate := ""
	found := false

	date := time.Now()
	// Starting from today
	for {
		lastDate = buildDatePrefix(date)
		if verbose {
			log.Println("Prefix: ", daapPrefix+"/"+lastDate)

		}
		objects := getS3Objects(daapRegion, daapBucketName, daapPrefix+"/"+lastDate, false)

		if lastDateAnyReport == "None" && len(objects.Contents) > 0 {
			lastDateAnyReport = formatOutputDate(date.AddDate(0, 0, -2))
		}

		if len(objects.Contents) != len(msoList)+1 && len(objects.Contents) != len(msoList) {
			date = date.AddDate(0, 0, -1)
			if gotToFar(date) {
				break
			}
			continue
		}
		found = true
		withAggregated := false

		msoCount := 0
		for _, key := range objects.Contents {
			if verbose {
				log.Println("Key: ", *key.Key)
			}

			if strings.Contains(*key.Key, "viewership-report-") {
				withAggregated = true
				continue
			}

			for _, mso := range msoList {
				if strings.Contains(*key.Key, mso.Name) {
					msoCount++
				}
			}
		}

		if msoCount != len(msoList) {
			found = false
		}

		if found {
			// That was the last successfull run, now add one day after if no aggregated reports
			if withAggregated {
				lastDate = formatOutputDate(date.AddDate(0, 0, 1))
			} else {
				lastDate = formatOutputDate(date)
			}

			break
		} else {
			date = date.AddDate(0, 0, -1)
			if gotToFar(date) {
				break
			}
			continue
		}
	}
	return found, lastDate, lastDateAnyReport
}

// getLastAvailable looks up for the last date available on CDW
func getLastAvailable() (bool, string) {

	lastDate := ""
	found := false
	date := time.Now()

	// Starting from today
	for {
		lastDate = buildDatePrefix(date)
		msoCount := 0
		// for each MSO
		for _, mso := range msoList {

			prefix := cdwPrefix + "/" + mso.Code + "/delta"
			objects := getS3Objects(cdwRegion, cdwBucketName, prefix, true)

			for _, key := range objects.Contents {
				// iterate through the list to match the dates range/mso name
				// using the constracted below lookup string
				if verbose {
					log.Println("Key: ", *key.Key)
				}
				if strings.Contains(*key.Key, "_"+lastDate) && *key.Size > 14 {
					msoCount++
				}
			}
		}
		if msoCount == len(msoList) {
			lastDate = formatOutputDate(date)
			found = true
			break
		} else {
			date = date.AddDate(0, 0, -1)
			if gotToFar(date) {
				break
			}
		}
	}
	return found, lastDate
}

func main() {
	var wg sync.WaitGroup

	if verbose {
		log.Printf("Params provided: -K %s, -S %s, -b %s, -d %s, -dp %s -v %v\n",
			cdwAwsAccessKey, cdwAwsSecretKey, cdwBucketName, daapBucketName, daapPrefix, verbose)
	}

	var foundDaap, foundCDW bool
	var maxAvailableDate, lastProcessedDate string

	if daapOnly {
		foundDaap, lastProcessedDate = getDatesForAggregates()
		fmt.Println(foundDaap, lastProcessedDate)
		os.Exit(0)
	}

	wg.Add(2)

	go func() {
		defer wg.Done()
		foundDaap, lastProcessedDate, _ = getLastDateFromDaap()
	}()

	go func() {
		defer wg.Done()
		foundCDW, maxAvailableDate = getLastAvailable()
	}()

	wg.Wait()

	fmt.Println(foundDaap, lastProcessedDate, foundCDW, maxAvailableDate)
}
