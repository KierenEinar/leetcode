package slidingwindow

import (
	"bytes"
	"math"
)

/**
给定一个字符串S和T, 请在S中找出含有T的最小子串

输入: S = "ADOBECODEBANC", T = "ABC"
输出: "BANC"

*/

func MinimumWindowSubstring(s, t string) string {

	var (
		window = make(map[byte]int16)
		left, right, count = 0, 0, len(t)
		res = bytes.NewBuffer(nil)
		minSubstringLen = math.MaxInt64
	)

	for i := range t {
		c := t[i]
		window[c]++
	}

	for right < len(s) {
		c := s[right]
		window[c]--
		if window[c]>=0 {
			count--
		}
		right++
		for count == 0 {
			if minSubstringLen > right - left {
				minSubstringLen = right - left
				res.Reset()
				res.WriteString(s[left: right])
			}
			d := s[left]
			window[d]++
			if window[d] > 0 {
				count++
			}
			left++
		}
	}
	if res.Len() == 0 {
		return ""
	}
	return res.String()
}



