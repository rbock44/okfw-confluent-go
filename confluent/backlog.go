package confluent

import "github.com/confluentinc/confluent-kafka-go/kafka"

//GetBacklog returns the messages left in the partition
func (kc *MessageConsumer) GetBacklog() (int, error) {
	var n int

	// Get the current assigned partitions.
	toppars, err := kc.Consumer.Assignment()
	if err != nil {
		return n, err
	}

	// Get the current offset for each partition, assigned to this consumer group.
	toppars, err = kc.Consumer.Committed(toppars, 5000)
	if err != nil {
		return n, err
	}

	// Loop over the topic partitions, get the high watermark for each toppar, and
	// subtract the current offset from that number, to get the total "lag". We
	// combine this value for each toppar to get the final backlog integer.
	var l, h int64
	for i := range toppars {
		l, h, err = kc.Consumer.QueryWatermarkOffsets(*toppars[i].Topic, toppars[i].Partition, 5000)
		if err != nil {
			return n, err
		}

		o := int64(toppars[i].Offset)
		if toppars[i].Offset == kafka.OffsetInvalid {
			o = l
		}

		n = n + int(h-o)
	}

	return n, nil
}
