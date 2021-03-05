package kt

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

type BytesEncoder interface {
	Encode(src []byte) string
}

type BytesEncoderFn func(src []byte) string

func (f BytesEncoderFn) Encode(src []byte) string {
	return f(src)
}

func ParseBytesEncoder(encoder string) (BytesEncoder, error) {
	switch encoder {
	case "hex":
		return BytesEncoderFn(hex.EncodeToString), nil
	case "base64":
		return BytesEncoderFn(base64.StdEncoding.EncodeToString), nil
	case "string":
		return BytesEncoderFn(func(data []byte) string { return string(data) }), nil
	}

	return nil, fmt.Errorf(`bad encoder %s, only allow string/hex/base64`, encoder)
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
