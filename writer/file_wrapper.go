package writer

import (
	"os"
	"strings"

	"github.com/bfgbot/superfetch/s3_utils"
)

// FileWrapper implements io.WriteCloser and is used by AvroWriter.
// It wraps an os.File and provides additional functionality for
// tracking file size and handling both local and S3 file destinations.
type FileWrapper struct {
	fp   *os.File
	size int
	dir  string
	file string
}

// NewFileWrapper creates a new FileWrapper instance.
// It handles both local and S3 file destinations.
// For S3, it creates a temporary local file.
// For local destinations, it ensures the directory exists and creates the file.
// It panics if any errors occur during file or directory creation.
func NewFileWrapper(dir string, file string) *FileWrapper {
	var fp *os.File
	var err error

	if strings.HasPrefix(dir, "s3://") {
		fp, err = os.CreateTemp("", "")
		if err != nil {
			panic(err)
		}
	} else {
		if err := os.MkdirAll(dir, 0755); err != nil {
			panic(err)
		}
		fp, err = os.Create(dir + file)
		if err != nil {
			panic(err)
		}
	}
	return &FileWrapper{fp: fp, dir: dir, file: file}
}

func (f *FileWrapper) Write(p []byte) (n int, err error) {
	n, err = f.fp.Write(p)
	f.size += n
	return
}

// Close closes the file and performs necessary cleanup.
// For S3 destinations, it uploads the file to S3 and removes the local temporary file.
// It panics if any errors occur during the closing process.
func (f *FileWrapper) Close() error {
	err := f.fp.Close()
	if err != nil {
		panic(err)
	}

	if strings.HasPrefix(f.dir, "s3://") {
		bucket, key := s3_utils.ParseS3Uri(f.dir + f.file)
		s3_utils.UploadFile(f.fp.Name(), bucket, key)
		err = os.Remove(f.fp.Name())
		if err != nil {
			panic(err)
		}
	}
	return nil
}
