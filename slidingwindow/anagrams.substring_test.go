package slidingwindow

import (
	"reflect"
	"testing"
)

func TestFindAnagramsInAString(t *testing.T) {
	type args struct {
		s string
		t string
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{
			name: "给定一个字符串 s 和一个非空字符串 p，找到 s 中所有是 p 的字母异位词的子串，返回这些子串的\n起始索引。",
			args: args{
				s: "cbaebabacd",
				t: "abc",
			},
			want: []int{0, 6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FindAnagramsInAString(tt.args.s, tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FindAnagramsInAString() = %v, want %v", got, tt.want)
			}
		})
	}
}
