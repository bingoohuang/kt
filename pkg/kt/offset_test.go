package kt

import (
	"reflect"
	"testing"

	"github.com/Shopify/sarama"
)

func TestParseOffsets(t *testing.T) {
	data := []struct {
		testName    string
		input       string
		expected    map[int32]OffsetInterval
		expectedErr string
	}{
		{
			testName: "empty",
			input:    "",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest},
					End:   Offset{Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "single-comma",
			input:    ",",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest},
					End:   Offset{Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "all",
			input:    "all",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "oldest",
			input:    "oldest",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "resume",
			input:    "resume",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: OffsetResume},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "all-with-space",
			input:    "	all ",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "all-with-zero-initial-Offset",
			input:    "all=+0:",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest, Diff: 0},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "several-partitions",
			input:    "1,2,4",
			expected: map[int32]OffsetInterval{
				1: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
				2: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
				4: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "one-partition,empty-offsets",
			input:    "0=",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "one-partition,one-Offset",
			input:    "0=1",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: false, Start: 1},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "one-partition,empty-after-colon",
			input:    "0=1:",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: false, Start: 1},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "multiple-partitions",
			input:    "0=4:,2=1:10,6",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: false, Start: 4},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
				2: {
					Start: Offset{Relative: false, Start: 1},
					End:   Offset{Relative: false, Start: 10},
				},
				6: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "newest-relative",
			input:    "0=-1",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: true, Start: sarama.OffsetNewest, Diff: -1},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "newest-relative,empty-after-colon",
			input:    "0=-1:",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: true, Start: sarama.OffsetNewest, Diff: -1},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "resume-relative",
			input:    "0=resume-10",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: true, Start: OffsetResume, Diff: -10},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "oldest-relative",
			input:    "0=+1",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest, Diff: 1},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "oldest-relative,empty-after-colon",
			input:    "0=+1:",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest, Diff: 1},
					End:   Offset{Relative: false, Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "oldest-relative-to-newest-relative",
			input:    "0=+1:-1",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest, Diff: 1},
					End:   Offset{Relative: true, Start: sarama.OffsetNewest, Diff: -1},
				},
			},
		},
		{
			testName: "specific-partition-with-all-partitions",
			input:    "0=+1:-1,all=1:10",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest, Diff: 1},
					End:   Offset{Relative: true, Start: sarama.OffsetNewest, Diff: -1},
				},
				-1: {
					Start: Offset{Relative: false, Start: 1, Diff: 0},
					End:   Offset{Relative: false, Start: 10, Diff: 0},
				},
			},
		},
		{
			testName: "oldest-to-newest",
			input:    "0=oldest:newest",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest, Diff: 0},
					End:   Offset{Relative: true, Start: sarama.OffsetNewest, Diff: 0},
				},
			},
		},
		{
			testName: "oldest-to-newest-with-offsets",
			input:    "0=oldest+10:newest-10",
			expected: map[int32]OffsetInterval{
				0: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest, Diff: 10},
					End:   Offset{Relative: true, Start: sarama.OffsetNewest, Diff: -10},
				},
			},
		},
		{
			testName: "newest",
			input:    "newest",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetNewest, Diff: 0},
					End:   Offset{Relative: false, Start: 1<<63 - 1, Diff: 0},
				},
			},
		},
		{
			testName: "single-partition",
			input:    "10",
			expected: map[int32]OffsetInterval{
				10: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest, Diff: 0},
					End:   Offset{Relative: false, Start: 1<<63 - 1, Diff: 0},
				},
			},
		},
		{
			testName: "single-range,all-partitions",
			input:    "10:20",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Start: 10},
					End:   Offset{Start: 20},
				},
			},
		},
		{
			testName: "single-range,all-partitions,open-End",
			input:    "10:",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Start: 10},
					End:   Offset{Start: 1<<63 - 1},
				},
			},
		},
		{
			testName: "all-newest",
			input:    "all=newest:",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetNewest, Diff: 0},
					End:   Offset{Relative: false, Start: 1<<63 - 1, Diff: 0},
				},
			},
		},
		{
			testName: "implicit-all-newest-with-Offset",
			input:    "newest-10:",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetNewest, Diff: -10},
					End:   Offset{Relative: false, Start: 1<<63 - 1, Diff: 0},
				},
			},
		},
		{
			testName: "implicit-all-oldest-with-Offset",
			input:    "oldest+10:",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest, Diff: 10},
					End:   Offset{Relative: false, Start: 1<<63 - 1, Diff: 0},
				},
			},
		},
		{
			testName: "implicit-all-neg-Offset-empty-colon",
			input:    "-10:",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetNewest, Diff: -10},
					End:   Offset{Relative: false, Start: 1<<63 - 1, Diff: 0},
				},
			},
		},
		{
			testName: "implicit-all-pos-Offset-empty-colon",
			input:    "+10:",
			expected: map[int32]OffsetInterval{
				-1: {
					Start: Offset{Relative: true, Start: sarama.OffsetOldest, Diff: 10},
					End:   Offset{Relative: false, Start: 1<<63 - 1, Diff: 0},
				},
			},
		},
		{
			testName:    "invalid-partition",
			input:       "bogus",
			expectedErr: `invalid Offset "bogus"`,
		},
		{
			testName:    "several-colons",
			input:       ":::",
			expectedErr: `invalid Offset "::"`,
		},
		{
			testName:    "bad-relative-Offset-Start",
			input:       "foo+20",
			expectedErr: `invalid Offset "foo+20"`,
		},
		{
			testName:    "bad-relative-Offset-diff",
			input:       "oldest+bad",
			expectedErr: `invalid Offset "oldest+bad"`,
		},
		{
			testName:    "bad-relative-Offset-diff-at-Start",
			input:       "+bad",
			expectedErr: `invalid Offset "+bad"`,
		},
		{
			testName:    "relative-Offset-too-big",
			input:       "+9223372036854775808",
			expectedErr: `Offset "+9223372036854775808" is too large`,
		},
		{
			testName:    "starting-Offset-too-big",
			input:       "9223372036854775808:newest",
			expectedErr: `Offset "9223372036854775808" is too large`,
		},
		{
			testName:    "ending-Offset-too-big",
			input:       "oldest:9223372036854775808",
			expectedErr: `Offset "9223372036854775808" is too large`,
		},
		{
			testName:    "partition-too-big",
			input:       "2147483648=oldest",
			expectedErr: `partition number "2147483648" is too large`,
		},
	}

	for _, d := range data {
		t.Run(d.testName, func(t *testing.T) {
			actual, err := ParseOffsets(d.input)
			if d.expectedErr != "" {
				if err == nil {
					t.Fatalf("got no error; want error %q", d.expectedErr)
				}
				if got, want := err.Error(), d.expectedErr; got != want {
					t.Fatalf("got unexpected error %q want %q", got, want)
				}
				return
			}
			if !reflect.DeepEqual(actual, d.expected) {
				t.Errorf(
					`
Expected: %+v, err=%v
Actual:   %+v, err=%v
Input:    %v
	`,
					d.expected,
					d.expectedErr,
					actual,
					err,
					d.input,
				)
			}
		})
	}
}
