package main

import (
	"fmt"
	"slices"
	"strings"
)

// Half-open range, like [start, end). `End` is not in the interval.
type Interval struct {
	Start, End uint64
}

type IntervalList []Interval

func ParseInterval(s string) Interval {
	parts := strings.Split(s, "-")
	return Interval{uint64(MustParseInt(parts[0])), uint64(MustParseInt(parts[1]))}
}

func (i Interval) String() string {
	return fmt.Sprintf("%d-%d", i.Start, i.End)
}

func CompareIntervals(i, j Interval) int {
	if i.Start < j.Start {
		return -1
	} else if i.Start > j.Start {
		return 1
	} else {
		return 0
	}
}

func (i Interval) Includes(j Interval) bool {
	return i.Start <= j.Start && i.End >= j.End
}

func ParseIntervalList(s string) IntervalList {
	if s == "" {
		return IntervalList{}
	}
	intervals := strings.Split(s, ",")
	cl := make(IntervalList, len(intervals))
	for i, interval := range intervals {
		cl[i] = ParseInterval(interval)
	}
	return cl
}

func (list IntervalList) Includes(interval Interval) bool {
	for _, i := range list {
		if i.Includes(interval) {
			return true
		}
	}
	return false
}

func (list IntervalList) String() string {
	strs := make([]string, len(list))
	for i, interval := range list {
		strs[i] = interval.String()
	}
	return strings.Join(strs, ",")
}

// Returns a copy of 'list' with the interval added.
func (list IntervalList) Merge(interval Interval) IntervalList {
	list = append(list, interval)
	slices.SortFunc(list, CompareIntervals)

	// Coalesce any two adjacent intervals whose start and end are the same.
	for i := 0; i < len(list) - 1; i++ {
		if list[i].End == list[i + 1].Start {
			list[i].End = list[i + 1].End
			list = DeleteFromSlice(list, i + 1)
			i--
		}
	}
	return list
}

// Returns a copy of 'list' with the given interval removed.
func (list IntervalList) Subtract(interval Interval) IntervalList {
	if len(list) == 0 || interval.End <= list[0].Start || interval.Start >= list[len(list) - 1].End {
		return list
	}

	result := IntervalList{}
	for _, orig := range list {
		if interval.Start <= orig.Start && interval.End >= orig.End {
			// this interval is completely deleted
		} else if orig.Start < interval.Start && orig.End > interval.End {
			result = result.Merge(Interval{orig.Start, interval.Start})
			result = result.Merge(Interval{interval.End, orig.End})
		} else if orig.Start >= interval.Start && orig.Start < interval.End && orig.End >= interval.End {
			result = result.Merge(Interval{interval.End, orig.End})
		} else if orig.Start <= interval.Start && orig.End > interval.Start {
			result = result.Merge(Interval{orig.Start, interval.Start})
		} else {
			result = result.Merge(orig)
		}
	}
	return result
}

func (list IntervalList) HighestContiguous() uint64 {
	if len(list) == 0 {
		return 0
	}
	return list[0].End
}

func (list IntervalList) NextGap(maxSize uint64) Interval {
	switch len(list) {
	case 0:
		return Interval{0, uint64(maxSize)}
	case 1:
		return Interval{list[0].End, list[0].End + uint64(maxSize)}
	default:
		if maxSize > list[1].Start - list[0].End {
			maxSize = list[1].Start - list[0].End
		}
		return Interval{list[0].End, list[0].End + uint64(maxSize)}
	}
}

