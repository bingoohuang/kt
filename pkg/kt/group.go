package kt

type GroupInfo struct {
	Name    string        `json:"name"`
	Topic   string        `json:"topic,omitempty"`
	Offsets []GroupOffset `json:"offsets,omitempty"`
}

type GroupOffset struct {
	Partition int32  `json:"partition"`
	Offset    *int64 `json:"Offset"`
	Lag       *int64 `json:"lag"`
}
