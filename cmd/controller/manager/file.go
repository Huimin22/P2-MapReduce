package manager

import (
	"dfs/utils"
	"time"
)

// FileMetadata is the metadata of a file
type FileMetadata struct {
	Name      string
	Chunks    []ChunkInfo
	Size      uint64
	CreatedAt time.Time
	IsText    bool
}

// Clone clone the FileMetadata
func (fm FileMetadata) Clone() FileMetadata {
	chunks := make([]ChunkInfo, 0, len(fm.Chunks))
	for i := range fm.Chunks {
		chunks = append(chunks, fm.Chunks[i].Clone())
	}
	return FileMetadata{
		Name:      fm.Name,
		Chunks:    chunks,
		Size:      fm.Size,
		CreatedAt: fm.CreatedAt,
		IsText:    fm.IsText,
	}
}

// ChunkInfo is the metadata of a chunk
type ChunkInfo struct {
	ChunkID uint64
	Hash    string
	// the first element of the replicas is the primary
	Replicas *utils.UnorderedList[string]
	Size     uint64
	fm       *FileMetadata
}

// Clone clone the ChunkInfo
func (c ChunkInfo) Clone() ChunkInfo {
	return ChunkInfo{
		ChunkID:  c.ChunkID,
		Hash:     c.Hash,
		Replicas: c.Replicas.Clone(),
		Size:     c.Size,
	}
}

// removeReplica removes a replica from the chunk
func (c ChunkInfo) removeReplica(nodeId string) {
	c.Replicas.Remove(nodeId)
}

// addReplica adds a replica to the chunk
func (c ChunkInfo) addReplica(nodeId string) {
	c.Replicas.AddNoDuplicate(nodeId)
}
