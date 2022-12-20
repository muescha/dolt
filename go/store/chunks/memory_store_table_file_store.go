// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunks

import (
	"bytes"
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/hash"
)

type MemoryStoreTableFile struct {
	fileID    string
	numChunks int
	data      []byte
}

func (mstf *MemoryStoreTableFile) FileID() string {
	return mstf.fileID
}

func (mstf *MemoryStoreTableFile) NumChunks() int {
	return mstf.numChunks
}

func (mstf *MemoryStoreTableFile) Open(ctx context.Context) (io.ReadCloser, uint64, error) {
	return io.NopCloser(bytes.NewReader(mstf.data)), uint64(len(mstf.data)), nil
}

type MemoryStoreTableFileWriter struct {
	fileID    string
	numChunks int
	writer    *bytes.Buffer
	msv       *MemoryStoreView
}

func (mstfWr *MemoryStoreTableFileWriter) Write(data []byte) (int, error) {
	return mstfWr.writer.Write(data)
}

func (mstfWr *MemoryStoreTableFileWriter) Close(ctx context.Context) error {
	data := mstfWr.writer.Bytes()
	mstfWr.writer = nil

	mstfWr.msv.mu.Lock()
	defer mstfWr.msv.mu.Unlock()
	mstfWr.msv.storage.tableFiles[mstfWr.fileID] = &MemoryStoreTableFile{mstfWr.fileID, mstfWr.numChunks, data}
	return nil
}

// Sources retrieves the current root hash, a list of all the table files (which may include appendix table files),
// and a second list containing only appendix table files.
func (ms *MemoryStoreView) Sources(ctx context.Context) (hash.Hash, []TableFile, []TableFile, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var tblFiles []TableFile
	for _, tblFile := range ms.storage.tableFiles {
		tblFiles = append(tblFiles, tblFile)
	}

	return ms.rootHash, tblFiles, []TableFile{}, nil
}

// Size  returns the total size, in bytes, of the table files in this Store.
func (ms *MemoryStoreView) Size(ctx context.Context) (uint64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	sz := uint64(0)
	for _, tblFile := range ms.storage.tableFiles {
		sz += uint64(len(tblFile.data))
	}
	return sz, nil
}

// WriteTableFile will read a table file from the provided reader and write it to the TableFileStore.
func (ms *MemoryStoreView) WriteTableFile(ctx context.Context, fileId string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error {
	tblFile := &MemoryStoreTableFileWriter{fileId, numChunks, bytes.NewBuffer(nil), ms}
	rd, _, err := getRd()
	if err != nil {
		return err
	}
	defer rd.Close()
	_, err = io.Copy(tblFile, rd)

	if err != nil {
		return err
	}

	return tblFile.Close(ctx)
}

// AddTableFilesToManifest adds table files to the manifest
func (ms *MemoryStoreView) AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int) error {
	return nil
}

// PruneTableFiles deletes old table files that are no longer referenced in the manifest.
func (ms *MemoryStoreView) PruneTableFiles(ctx context.Context) error {
	return ErrUnsupportedOperation
}

// SetRootChunk changes the root chunk hash from the previous value to the new root.
func (ms *MemoryStoreView) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	ms.rootHash = root
	return nil
}

// SupportedOperations returns a description of the support TableFile operations. Some stores only support reading table files, not writing.
func (ms *MemoryStoreView) SupportedOperations() TableFileStoreOps {
	return TableFileStoreOps{
		CanRead:  true,
		CanWrite: true,
	}
}
