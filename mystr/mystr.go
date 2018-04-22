package mystr

import (
	"fmt"
	"strings"
)

func SliceToStringUint32(t []uint32, sep string) string {
	arr := make([]string, len(t))

	for i, v := range t {
		arr[i] = fmt.Sprintf("%d", v)
	}

	return strings.Join(arr, sep)
}
func SliceToStringUint64(t []uint64, sep string) string {
	arr := make([]string, len(t))

	for i, v := range t {
		arr[i] = fmt.Sprintf("%d", v)
	}

	return strings.Join(arr, sep)
}

func Uin32SliceToStringSlice(t []uint32) []string {
	arr := make([]string, len(t))

	for i, v := range t {
		arr[i] = fmt.Sprintf("%d", v)
	}

	return arr
}
