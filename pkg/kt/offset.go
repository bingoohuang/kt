package kt

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
)

const OffsetResume int64 = -3

type Offset struct {
	Expr     string
	Start    int64
	Diff     int64
	Relative bool
}

func (o Offset) String() string {
	if o.Relative {
		if o.Diff != 0 {
			return fmt.Sprintf("%s%+d", o.Expr, o.Diff)
		}
		return o.Expr
	}

	if o.Expr != "" {
		return o.Expr
	}
	return fmt.Sprintf("%d", o.Start)
}

func OldestOffset() Offset { return Offset{Relative: true, Start: sarama.OffsetOldest, Expr: "Oldest"} }
func NewestOffset() Offset { return Offset{Relative: true, Start: sarama.OffsetNewest, Expr: "Newest"} }
func LastOffset() Offset   { return Offset{Relative: false, Start: 1<<63 - 1, Expr: "1<<63-1"} }

type OffsetInterval struct {
	Start Offset
	End   Offset
}

// ParseOffsets parses a set of partition-Offset specifiers in the following
// syntax. The grammar uses the BNF-like syntax defined in https://golang.org/ref/spec.
//
//	offsets := [ partitionInterval { "," partitionInterval } ]
//
//	partitionInterval :=
//		partition "=" OffsetInterval |
//		partition |
//		OffsetInterval
//
//	partition := "all" | number
//
//	OffsetInterval := [ Offset ] [ ":" [ Offset ] ]
//
//	Offset :=
//		number |
//		namedRelativeOffset |
//		numericRelativeOffset |
//		namedRelativeOffset numericRelativeOffset
//
//	namedRelativeOffset := "newest" | "oldest" | "resume"
//
//	numericRelativeOffset := "+" number | "-" number
//
//	number := {"0"| "1"| "2"| "3"| "4"| "5"| "6"| "7"| "8"| "9"}
func ParseOffsets(str string) (map[int32]OffsetInterval, error) {
	result := map[int32]OffsetInterval{}
	for _, partitionInfo := range strings.Split(str, ",") {
		partitionInfo = strings.TrimSpace(partitionInfo)
		// There's a grammatical ambiguity between a partition
		// number and an OffsetInterval, because both allow a single
		// decimal number. We work around that by trying an explicit
		// partition first.
		p, err := parsePartition(partitionInfo)
		if err == nil {
			result[p] = OffsetInterval{Start: OldestOffset(), End: LastOffset()}
			continue
		}
		intervalStr := partitionInfo
		if i := strings.Index(partitionInfo, "="); i >= 0 {
			// There's an explicitly specified partition.
			p, err = parsePartition(partitionInfo[0:i])
			if err != nil {
				return nil, err
			}
			intervalStr = partitionInfo[i+1:]
		} else {
			// No explicit partition, so implicitly use "all".
			p = -1
		}
		intv, err := parseInterval(intervalStr)
		if err != nil {
			return nil, err
		}
		result[p] = intv
	}
	return result, nil
}

// parsePartition parses a partition number, or the special word "all", meaning all partitions.
func parsePartition(s string) (int32, error) {
	if s == "all" {
		return -1, nil
	}
	p, err := strconv.ParseUint(s, 10, 31)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, fmt.Errorf("partition number %q is too large", s)
		}
		return 0, fmt.Errorf("invalid partition number %q", s)
	}
	return int32(p), nil
}

func parseInterval(s string) (OffsetInterval, error) {
	if s == "" {
		// An empty string implies all messages.
		return OffsetInterval{
			Start: OldestOffset(),
			End:   LastOffset(),
		}, nil
	}
	var start, end string
	i := strings.Index(s, ":")
	if i == -1 {
		// No colon, so the whole string specifies the Start Offset.
		start = s
	} else {
		// We've got a colon, so there are explicitly specified
		// Start and End offsets.
		start = s[0:i]
		end = s[i+1:]
	}
	startOff, err := parseIntervalPart(start, OldestOffset())
	if err != nil {
		return OffsetInterval{}, err
	}
	endOff, err := parseIntervalPart(end, LastOffset())
	if err != nil {
		return OffsetInterval{}, err
	}
	return OffsetInterval{
		Start: startOff,
		End:   endOff,
	}, nil
}

// parseIntervalPart parses one half of an OffsetInterval pair.
// If s is empty, the given default Offset will be used.
func parseIntervalPart(s string, defaultOffset Offset) (Offset, error) {
	if s == "" {
		return defaultOffset, nil
	}
	n, err := strconv.ParseUint(s, 10, 63)
	if err == nil {
		// It's an explicit numeric Offset.
		return Offset{
			Start: int64(n),
		}, nil
	}
	if errors.Is(err, strconv.ErrRange) {
		return Offset{}, fmt.Errorf("offset %q is too large", s)
	}
	o, err := parseRelativeOffset(s)
	if err != nil {
		return Offset{}, err
	}
	return o, nil
}

// parseRelativeOffset parses a relative Offset, such as "oldest", "newest-30", or "+20".
func parseRelativeOffset(s string) (Offset, error) {
	o, ok := parseNamedOffset(s)
	if ok {
		return o, nil
	}
	i := strings.IndexAny(s, "+-")
	if i == -1 {
		return Offset{}, fmt.Errorf("invalid Offset %q", s)
	}
	switch {
	case i > 0:
		// The + or - isn't at the Start, so the relative Offset must Start
		// with a named relative Offset.
		o, ok = parseNamedOffset(s[0:i])
		if !ok {
			return Offset{}, fmt.Errorf("invalid Offset %q", s)
		}
	case s[i] == '+':
		// Offset +99 implies oldest+99.
		o = OldestOffset()
	default:
		// Offset -99 implies newest-99.
		o = NewestOffset()
	}
	// Note: we include the leading sign when converting to int
	// so the diff ends up with the correct sign.
	diff, err := strconv.ParseInt(s[i:], 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return Offset{}, fmt.Errorf("offset %q is too large", s)
		}
		return Offset{}, fmt.Errorf("invalid Offset %q", s)
	}
	o.Diff = diff
	return o, nil
}

func parseNamedOffset(s string) (Offset, bool) {
	switch s {
	case "newest":
		return NewestOffset(), true
	case "oldest":
		return OldestOffset(), true
	case "resume":
		return Offset{Relative: true, Start: OffsetResume}, true
	default:
		return Offset{}, false
	}
}
