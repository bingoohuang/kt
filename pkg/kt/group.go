package kt

type GroupInfo struct {
	Group   string        `json:"group"`
	Topic   string        `json:"topic,omitempty"`
	Offsets []GroupOffset `json:"offsets,omitempty"`
}

type GroupOffset struct {
	Metadata        string `json:"metadata,omitempty"`
	GroupOffset     int64  `json:"groupOffset"`
	PartitionOffset int64  `json:"partitionOffset"`
	Lag             int64  `json:"lag"`
	Partition       int32  `json:"partition"`
}
