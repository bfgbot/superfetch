package reader

import (
	"log"

	"github.com/hamba/avro/v2/ocf"
)

type Input struct {
	URL string `avro:"url"`
}

// LoadUrls reads URLs from input Avro files and returns a channel of strings.
// It processes each input file, decodes the Avro records, and sends the URLs
// through the returned channel. This function is designed to be used
// concurrently, as it runs in a separate goroutine.

func LoadUrls(inputFiles []string) <-chan string {
	urls := make(chan string)
	go func() {
		defer close(urls)

		for _, inputFile := range inputFiles {
			fp := NewFileWrapper(inputFile)

			dec, err := ocf.NewDecoder(fp)
			if err != nil {
				log.Panicln("Failed to create decoder:", err)
			}

			for dec.HasNext() {
				var input Input
				err = dec.Decode(&input)
				if err != nil {
					log.Panicln("Failed to decode input:", inputFile, err)
				}

				urls <- input.URL
			}

			if dec.Error() != nil {
				log.Panicln("Failed to decode input:", inputFile, dec.Error())
			}
			fp.Close()
		}
	}()
	return urls
}
