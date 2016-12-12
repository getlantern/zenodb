package core

import (
	"bytes"
	"strings"
)

func FormatSource(source Source) string {
	result := &bytes.Buffer{}
	doFormatSource(result, "", source)
	return result.String()
}

func doFormatSource(result *bytes.Buffer, indent string, source Source) {
	for i, s := range strings.Split(source.String(), "\n") {
		result.WriteString(indent)
		if i == 0 {
			result.WriteString("<- ")
		}
		result.WriteString(s)
		result.WriteByte('\n')
	}
	t, ok := source.(Transform)
	if ok {
		indent += "  "
		s := t.GetSource()
		doFormatSource(result, indent, s)
	}
}
