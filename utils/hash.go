package utils

import (
	"crypto/md5"
	"encoding/hex"
	"io"
)

// HashBytes returns the MD5 hash of the given data.
func HashBytes(data []byte) (string, error) {
	h := md5.New()
	_, err := h.Write(data)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// HashReader returns the MD5 hash of the given reader.
func HashReader(r io.Reader) (string, error) {
	h := md5.New()
	_, err := io.Copy(h, r)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
