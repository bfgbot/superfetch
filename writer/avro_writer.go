package writer

import (
	"math/rand"
	"sync"

	"github.com/hamba/avro/v2/ocf"
)

// SPLIT_SIZE is the maximum size of an Avro file in bytes.
// The end file will be slightly larger.
const SPLIT_SIZE = 100_000_000

// newFile generates a random file name, for example:
// WsSdh.avro
func newFile() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 5)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b) + ".avro"
}

// AvroWriter is a struct that facilitates writing data in Apache Avro format.
// It provides functionality to write Avro-encoded data to files, handling file
// splitting based on size and managing concurrent writes.
//
// Key features:
// - Writes data in Avro Object Container File (OCF) format
// - Automatically splits files when they reach a specified size (SPLIT_SIZE)
// - Supports concurrent writing through mutex-based synchronization
// - Manages file creation, encoding, and closure
// - Uses ZStandard compression for efficient storage
//
// The AvroWriter is particularly useful for scenarios where large amounts of
// structured data need to be serialized efficiently, such as in big data
// processing pipelines or when storing data for later analysis.

type AvroWriter struct {
	dir    string
	schema string

	f   *FileWrapper
	enc *ocf.Encoder

	files []string

	mu sync.Mutex
}

func NewAvroWriter(dir string, schema string) *AvroWriter {
	return &AvroWriter{dir: dir, schema: schema}
}

// createSlice closes the current Avro file and opens a new one.
func (w *AvroWriter) createSlice() {
	w.FinalizeSlice()

	w.f = NewFileWrapper(w.dir, newFile())

	var err error
	w.enc, err = ocf.NewEncoder(w.schema, w.f, ocf.WithCodec(ocf.ZStandard))
	if err != nil {
		panic(err)
	}
}

// FinalizeSlice closes the current Avro file and returns the list of files created.
// It ensures all data is flushed and the file is properly closed.
func (w *AvroWriter) FinalizeSlice() []string {
	if w.f != nil {
		err := w.enc.Close()
		if err != nil {
			panic(err)
		}
		w.enc = nil

		w.files = append(w.files, w.f.file)

		w.f.Close()
		w.f = nil
	}
	return w.files
}

// Append adds a new record to the Avro file.
// If the current file reaches the SPLIT_SIZE, it closes the current file and opens a new one.
func (w *AvroWriter) Append(data any) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.f == nil {
		w.createSlice()
	}

	err := w.enc.Encode(data)
	if err != nil {
		panic(err)
	}

	if w.f.size >= SPLIT_SIZE {
		w.FinalizeSlice()
	}
}
