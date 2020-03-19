package tenant

import (
	"github.com/influxdata/influxdb"
)

// findOptionsParams converts find options into a paramiterizated key pair
func findOptionParams(opts ...influxdb.FindOptions) [][2]string {
	var out [][2]string
	for _, o := range opts {
		for k, vals := range o.QueryParams() {
			for _, v := range vals {
				out = append(out, [2]string{k, v})
			}
		}
	}
	return out
}
