package kt

import (
	"encoding/json"

	"github.com/bingoohuang/jj"
)

func ColorJSON(data any) []byte {
	jsonData, _ := json.Marshal(data)
	return jj.Color(jsonData, nil, nil)
}
