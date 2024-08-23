package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bfgbot/superfetch/reader"
	"github.com/bfgbot/superfetch/writer"
)

// TIMEOUT(seconds) is the maximum time to wait for a request to complete
const TIMEOUT = 10

// CONCURRENCY is the number of concurrent requests to make
const CONCURRENCY = 5

// httpClient is configured with a timeout and a policy to not follow redirects
var httpClient *http.Client

func newHttpClient() *http.Client {
	return &http.Client{
		Timeout: time.Duration(TIMEOUT) * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // Do not follow redirects
		},
	}
}

// newRequest creates a new HTTP GET request for the given URL
func newRequest(url string) *http.Request {
	r, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	r.Header.Set("user-agent", "bfgbot")
	return r
}

type Result struct {
	Url     string  `avro:"url"`
	Content []byte  `avro:"content"`
	Err     *string `avro:"err"`
}

func (r *Result) Load() {
	req := newRequest(r.Url)

	res, err := httpClient.Do(req)
	if err != nil {
		errStr := err.Error()
		r.Err = &errStr
		return
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		errStr := fmt.Sprint("status:", res.StatusCode)
		r.Err = &errStr
		return
	}

	content, err := io.ReadAll(res.Body)
	if err != nil {
		errStr := err.Error()
		r.Err = &errStr
		return
	}
	r.Content = content
}

var outputSchema = `{
  "type": "record",
  "name": "Result",
  "fields": [
    {
      "name": "url",
      "type": "string"
    },
    {
      "name": "content",
      "type": ["null", "bytes"]
    },
    {
      "name": "err",
      "type": ["null", "string"],
      "default": null
    }
  ]
}`

// Args:
//   - outputDir: S3 URI (s3://) or local directory for output
//   - inputFiles: List of Avro files containing URLs to fetch
//
// It reads URLs from input files, fetches content for each URL,
// and writes results to Avro files in the output directory.
// For S3 output, it uploads files after writing locally.
func main() {
	// Parse args
	if len(os.Args) < 2 {
		log.Fatalln("superfetch [output dir] [input files]")
	}

	outputDir := os.Args[1]
	if !strings.HasSuffix(outputDir, "/") {
		outputDir += "/"
	}
	inputFiles := os.Args[2:]

	// Setup
	setUlimit()
	httpClient = newHttpClient()
	urls := reader.LoadUrls(inputFiles)
	writer := writer.NewAvroWriter(outputDir, outputSchema)

	// Start workers
	sem := make(chan bool, CONCURRENCY)
	for url := range urls {
		sem <- true
		go func() {
			result := Result{Url: url}
			result.Load()
			writer.Append(result)
			<-sem
		}()
	}

	// Wait for all tasks to be done
	for range CONCURRENCY {
		sem <- true
	}

	// Print output files
	files := writer.FinalizeSlice()
	for _, file := range files {
		fmt.Println(file)
	}
}
