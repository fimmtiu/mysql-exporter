package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseIntervalList(t *testing.T) {
	list := ParseIntervalList("")
	assert.Equal(t, IntervalList{}, list)

	list = ParseIntervalList("0-200,500-600,10010-31337")
	expected := IntervalList{
		Interval{0, 200},
		Interval{500, 600},
		Interval{10010, 31337},
	}
	assert.Equal(t, expected, list)
}

func TestIntervalListString(t *testing.T) {
	assert.Equal(t, "", IntervalList{}.String())

	list := IntervalList{
		Interval{0, 200},
		Interval{500, 600},
		Interval{10010, 31337},
	}
	assert.Equal(t, "0-200,500-600,10010-31337", list.String())
}

func TestIntervalListMerge(t *testing.T) {
	list := IntervalList{
		Interval{0, 200},
		Interval{500, 600},
	}
	list = list.Merge(Interval{200, 300})
	assert.Equal(t, "0-300,500-600", list.String())
	list = list.Merge(Interval{600, 700})
	assert.Equal(t, "0-300,500-700", list.String())
	list = list.Merge(Interval{300, 400})
	assert.Equal(t, "0-400,500-700", list.String())
	list = list.Merge(Interval{400, 500})
	assert.Equal(t, "0-700", list.String())
	list = list.Merge(Interval{10_000, 10_100})
	assert.Equal(t, "0-700,10000-10100", list.String())
}

// Lots of edge cases here, so we have to test them all.
func TestIntervalListSubtract(t *testing.T) {
	// Subtraction from an empty list. (No-op.)
	result := IntervalList{}.Subtract(Interval{0, 50})
	assert.Equal(t, "", result.String())

	list := IntervalList{Interval{100,200}, Interval{300,400}}

	// The subtraction comes before all the intervals in the list. (No-op.)
	result = list.Subtract(Interval{0, 50})
	assert.Equal(t, "100-200,300-400", result.String())

	// The subtraction overlaps with the start of the first interval.
	result = list.Subtract(Interval{50, 150})
	assert.Equal(t, "150-200,300-400", result.String())

	// The subtraction is aligned with the start of the first interval.
	result = list.Subtract(Interval{100, 150})
	assert.Equal(t, "150-200,300-400", result.String())

	// The subtraction is entirely inside the first interval.
	result = list.Subtract(Interval{125, 175})
	assert.Equal(t, "100-125,175-200,300-400", result.String())

	// The subtraction is aligned with the end of the first interval.
	result = list.Subtract(Interval{150, 200})
	assert.Equal(t, "100-150,300-400", result.String())

	// The subtraction overlaps the end of the first interval.
	result = list.Subtract(Interval{150, 250})
	assert.Equal(t, "100-150,300-400", result.String())

	// The subtraction completely overlaps the first interval.
	result = list.Subtract(Interval{50, 250})
	assert.Equal(t, "300-400", result.String())
	result = list.Subtract(Interval{100, 250})
	assert.Equal(t, "300-400", result.String())

	// The subtraction falls in between the two intervals. (No-op.)
	result = list.Subtract(Interval{200, 300})
	assert.Equal(t, "100-200,300-400", result.String())

	// The subtraction overlaps the start of the second interval.
	result = list.Subtract(Interval{250, 350})
	assert.Equal(t, "100-200,350-400", result.String())

	// The subtraction is aligned with the start of the second interval.
	result = list.Subtract(Interval{300, 350})
	assert.Equal(t, "100-200,350-400", result.String())

	// The subtraction is entirely inside the second interval.
	result = list.Subtract(Interval{325, 375})
	assert.Equal(t, "100-200,300-325,375-400", result.String())

	// The subtraction is aligned with the end of the second interval.
	result = list.Subtract(Interval{350, 400})
	assert.Equal(t, "100-200,300-350", result.String())

	// The subtraction overlaps the end of the second interval.
	result = list.Subtract(Interval{350, 450})
	assert.Equal(t, "100-200,300-350", result.String())

	// The subtraction completely overlaps the second interval.
	result = list.Subtract(Interval{250, 400})
	assert.Equal(t, "100-200", result.String())
	result = list.Subtract(Interval{250, 500})
	assert.Equal(t, "100-200", result.String())

	// The subtraction comes after all the intervals in the list. (No-op.)
	result = list.Subtract(Interval{500, 600})
	assert.Equal(t, "100-200,300-400", result.String())

	// Phew!
}


func TestIntervalListNextGap(t *testing.T) {
	s := IntervalList{}.NextGap(1000).String()
	assert.Equal(t, "0-1000", s)
	s = IntervalList{Interval{0, 2000}}.NextGap(1000).String()
	assert.Equal(t, "2000-3000", s)
	s = IntervalList{Interval{0, 2000}, Interval{4000, 5000}}.NextGap(1000).String()
	assert.Equal(t, "2000-3000", s)
	s = IntervalList{Interval{0, 2000}, Interval{4000, 5000}}.NextGap(5000).String()
	assert.Equal(t, "2000-4000", s)
}
