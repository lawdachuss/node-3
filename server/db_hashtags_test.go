package server

import (
	"reflect"
	"testing"
)

func TestMergeHashtags(t *testing.T) {
	tests := []struct {
		name      string
		roomTitle string
		tags      []string
		want      []string
	}{
		{
			name:      "hashtags from title merged with provided tags",
			roomTitle: "Welcome #lovense #Milf #bigboobs thanks",
			tags:      []string{"Anal", "cum"},
			want:      []string{"anal", "cum", "lovense", "milf", "bigboobs"},
		},
		{
			name:      "empty title keeps provided tags normalized",
			roomTitle: "",
			tags:      []string{"Anal", "CUM"},
			want:      []string{"anal", "cum"},
		},
		{
			name:      "dedupe across title and provided tags",
			roomTitle: "#latin #anal",
			tags:      []string{"Latina", "anal", "LATIN"},
			want:      []string{"latina", "anal", "latin"},
		},
		{
			name:      "no hashtags and no tags yields empty",
			roomTitle: "private room only",
			tags:      nil,
			want:      []string{},
		},
		{
			name:      "numeric and underscore hashtags kept",
			roomTitle: "#18 #fuck_machine",
			tags:      nil,
			want:      []string{"18", "fuck_machine"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeHashtags(tt.roomTitle, tt.tags)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeHashtags() = %v, want %v", got, tt.want)
			}
		})
	}
}
