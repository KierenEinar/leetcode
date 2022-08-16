package slidingwindow
/**
给定一个字符串 s 和一个非空字符串 p，找到 s 中所有是 p 的字母异位词的子串，返回这些子串的
起始索引。(find-all-anagrams-in-a-string)

输入:
s: "cbaebabacd" p: "abc"

输出:
[0, 6]
解释:
起始索引等于 0 的子串是 "cba", 它是 "abc" 的字母异位词。
起始索引等于 6 的子串是 "bac", 它是 "abc" 的字母异位词。

**/

func FindAnagramsInAString(s, t string) []int {

	var (
		window = make(map[byte]int16)
		left, right, count = 0, 0, len(t)
		index = make([]int, 0)
	)

	for idx := range t {
		c := t[idx]
		window[c]+=1
	}

	for right < len(s) {

		c := s[right]
		window[c]--
		if window[c] >= 0 {
			count--
		}

		for count == 0 {
			if right-left == len(t)-1 {
				index = append(index, left)
			}
			d := s[left]
			window[d]++
			if window[d] > 0 {
				count++
			}
			left++
		}
		right++
	}

	return index
}
