package slidingwindow

import "testing"

func TestMinimumWindowSubstring(t *testing.T) {

	type args struct {
		s string
		t string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		 {
		 	name: "输入: S = \"ADOBECODEBANC\", T = \"ABC\"\n输出: \"BANC\"",
		 	args: struct {
				s string
				t string
			}{s: "ADOBECODEBANC", t: "ABC"},
			want: "BANC",
		 },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MinimumWindowSubstring(tt.args.s, tt.args.t); got != tt.want {
				t.Errorf("minimumWindowSubstring() = %v, want %v", got, tt.want)
			}
		})
	}
}
