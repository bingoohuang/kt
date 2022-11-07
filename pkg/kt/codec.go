package kt

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

type BytesEncoder interface {
	Encode(src []byte) string
}

type BytesEncoderFn func(src []byte) string

func (f BytesEncoderFn) Encode(src []byte) string {
	return f(src)
}

type BytesEncoderFns []BytesEncoderFn

func (fns BytesEncoderFns) Encode(src []byte) string {
	result := ""
	for _, fn := range fns {
		if r := fn(src); result != "" {
			result += " " + r
		} else {
			result = r
		}
	}

	return result
}

func ParseBytesEncoder(encoder string) BytesEncoder {
	var fns BytesEncoderFns
	encoder = strings.ToLower(encoder)
	if strings.Contains(encoder, "hex") {
		fns = append(fns, func(src []byte) string {
			return "hex: " + hex.EncodeToString(src)
		})
	}

	if strings.Contains(encoder, "base64") {
		fns = append(fns, func(src []byte) string {
			return "base64: " + base64.StdEncoding.EncodeToString(src)
		})
	}

	if strings.Contains(encoder, "string") {
		fns = append(fns, func(data []byte) string {
			return "string: " + string(data)
		})
	}

	return fns
}

type StringDecoder interface {
	Decode(string) ([]byte, error)
}

type StringDecoderFn func(string) ([]byte, error)

func (f StringDecoderFn) Decode(s string) ([]byte, error) {
	return f(s)
}

func ParseStringDecoder(decoder string) (StringDecoder, error) {
	switch decoder {
	case "hex":
		return StringDecoderFn(hex.DecodeString), nil
	case "base64":
		return StringDecoderFn(base64.StdEncoding.DecodeString), nil
	case "string":
		return StringDecoderFn(func(s string) ([]byte, error) { return []byte(s), nil }), nil
	}
	return nil, fmt.Errorf(`bad decoder %s, only allow string/hex/base64`, decoder)
}
