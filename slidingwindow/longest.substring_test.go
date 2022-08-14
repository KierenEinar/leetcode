package slidingwindow

import "testing"

func TestLongestSubstring(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "输入: \"abcabcbb\" 输出: 3 解释: 因为无重复字符的最长子串是 \"abc\"，所以其长度为 3。",
			args: struct{ s string }{s: "abcabcbb"},
			want: "abc",
		},
		{
			name: "输入: \"bbbbb\" 输出: 1 解释: 因为无重复字符的最长子串是 \"b\"，所以其长度为 1。",
			args: struct{ s string }{s: "bbbbb"},
			want: "b",
		},
		{
			name: "输入: \"pwwkew\" 输出: 3 解释: 因为无重复字符的最长子串是 \"wke\" ",
			args: struct{ s string }{s: "pwwkew"},
			want: "wke",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LongestSubstring(tt.args.s); got != tt.want {
				t.Errorf("LongestSubstring() = %v, want %v", got, tt.want)
			}
		})
	}
}
