package write

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCsvData checks data that are writen in an annotated CSV file
func Test_CsvToProtocolLines_success(t *testing.T) {
	var tests = []struct {
		name  string
		csv   string
		lines string
		err   string
	}{
		{
			"simple1",
			"_measurement,a,b\ncpu,1,1\ncpu,b2\n",
			"cpu a=1,b=1\ncpu a=b2\n",
			"",
		},
		{
			"simple1_withSep",
			"sep=;\n_measurement;a;b\ncpu;1;1\ncpu;b2\n",
			"cpu a=1,b=1\ncpu a=b2\n",
			"",
		},
		{
			"simple2",
			"_measurement,a,b\ncpu,1,1\ncpu,\n",
			"",
			"no field data",
		},
		{
			"simple3",
			"_measurement,a,_time\ncpu,1,1\ncpu,2,x\n",
			"",
			"invalid syntax", // x is not valid for time column
		},
	}
	bufferSizes := []int{40, 7, 3, 1}

	for _, test := range tests {
		for _, bufferSize := range bufferSizes {
			t.Run(test.name+"_"+string(bufferSize), func(t *testing.T) {
				reader := CsvToProtocolLines(strings.NewReader(test.csv))
				buffer := make([]byte, bufferSize)
				lines := make([]byte, 0, 100)
				for {
					n, err := reader.Read(buffer)
					if err != nil {
						if err == io.EOF {
							break
						}
						if test.err != "" {
							if err := err.Error(); !strings.Contains(err, test.err) {
								require.Equal(t, err, test.err)
							}
							return
						}
						require.Nil(t, err.Error())
						break
					}
					lines = append(lines, buffer[:n]...)
				}
				if test.err == "" {
					require.Equal(t, test.lines, string(lines))
				} else {
					require.Fail(t, "error message with '"+test.err+"' expected")
				}
			})
		}
	}
}
