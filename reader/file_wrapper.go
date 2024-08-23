package reader

import (
	"log"
	"os"
	"strings"

	"github.com/bfgbot/superfetch/s3_utils"
)

type FileWrapper struct {
	fp            *os.File
	deleteOnClose bool
}

// FileWrapper is a struct that wraps a file pointer (os.File) and provides
// additional functionality for handling both local and S3-stored files.
//
// It implements the io.Reader interface, allowing it to be used in place
// of standard file readers. The struct also keeps track of whether the
// file should be deleted after closing, which is useful for temporary
// files downloaded from S3.
//
// Key features:
// - Supports reading from both local files and S3 objects
// - Automatically handles downloading from S3 when necessary
// - Implements io.Reader for compatibility with other Go libraries
// - Manages cleanup of temporary files downloaded from S3

func NewFileWrapper(uri string) *FileWrapper {
	var localFile string
	var deleteOnClose bool

	if strings.HasPrefix(uri, "s3://") {
		bucket, key := s3_utils.ParseS3Uri(uri)
		localFile = s3_utils.DownloadFile(bucket, key)
		deleteOnClose = true
	} else {
		localFile = uri
		deleteOnClose = false
	}

	fp, err := os.Open(localFile)
	if err != nil {
		log.Panicln("Failed to open file:", localFile, err)
	}
	return &FileWrapper{fp: fp, deleteOnClose: deleteOnClose}
}

func (r *FileWrapper) Read(p []byte) (n int, err error) {
	return r.fp.Read(p)
}

func (r *FileWrapper) Close() error {
	err := r.fp.Close()
	if err != nil {
		return err
	}
	if r.deleteOnClose {
		err = os.Remove(r.fp.Name())
		if err != nil {
			return err
		}
	}
	return nil
}
