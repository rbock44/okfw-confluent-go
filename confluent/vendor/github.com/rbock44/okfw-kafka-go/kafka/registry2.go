package kafka

import (
	"encoding/binary"
	"fmt"
	"io"
)

//readSchemaID of message
func readSchemaID(reader io.Reader) (uint32, error) {
	headerBytes := [5]byte{}
	len, err := reader.Read(headerBytes[:])
	if err != nil {
		return 0, fmt.Errorf("error reading header [#%v]", err)
	}
	if len != 5 {
		return 0, fmt.Errorf("header expected [5] but was [%d] bytes", len)
	}

	if headerBytes[0] != 0 {
		return 0, fmt.Errorf("header magic byte is not [0] but was [%d]", headerBytes[0])
	}
	messageSchemaID := binary.BigEndian.Uint32(headerBytes[1:5])

	return messageSchemaID, nil
}
