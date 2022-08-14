package slidingwindow

import (
	"bytes"
)

/**
无重复字符的最长子串-leetcode

给定一个字符串，请你找出其中不含有重复字符的 最长子串 的长度。

示例 1:

输入: "abcabcbb" 输出: 3 解释: 因为无重复字符的最长子串是 "abc"，所以其长度为 3。 示例 2:

输入: "bbbbb" 输出: 1 解释: 因为无重复字符的最长子串是 "b"，所以其长度为 1。 示例 3:

输入: "pwwkew" 输出: 3 解释: 因为无重复字符的最长子串是 "wke"，所以其长度为 3。 请注意，你的答案必须是 子串 的长度，"pwke" 是一个子序列，不是子串。

#


*/

func LongestSubstring(s string) string {

	var (
		window = make(map[byte]int16)
		left, right = 0, 0
		res = bytes.NewBuffer(nil)
		maxSubstring = 0
	)

	for right < len(s) {

		c := s[right]
		window[c]++

		for window[c] > 1 {

			if maxSubstring < right - left  {
				maxSubstring = right - left
				res.Reset()
				res.WriteString(s[left: right])
			}

			d := s[left]
			window[d]--
			left++
		}

		right++

	}

	if res.Len() == 0 {
		return ""
	}
	return res.String()
}


