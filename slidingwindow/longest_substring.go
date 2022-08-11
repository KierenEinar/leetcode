package slidingwindow

/**
无重复字符的最长子串-leetcode

给定一个字符串，请你找出其中不含有重复字符的 最长子串 的长度。

示例 1:

输入: "abcabcbb" 输出: 3 解释: 因为无重复字符的最长子串是 "abc"，所以其长度为 3。 示例 2:

输入: "bbbbb" 输出: 1 解释: 因为无重复字符的最长子串是 "b"，所以其长度为 1。 示例 3:

输入: "pwwkew" 输出: 3 解释: 因为无重复字符的最长子串是 "wke"，所以其长度为 3。 请注意，你的答案必须是 子串 的长度，"pwke" 是一个子序列，不是子串。

#


*/

func LengthLongestSubstring(s string) int {

	var (
		window = make(map[byte]int)
		left = 0
		right = 0
		l = len(s)
		max_ = 0
	)

	for right < l {
		c := s[right]
		window[c]++
		right++
		for window[c] > 1 {
			d := s[left]
			window[d]--
			left++
		}
		max_ = max(max_, right-left+1)
	}

	return max_
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
