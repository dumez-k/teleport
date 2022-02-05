/*MIT License

Copyright (c) 2019 Ulysse Carion

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package uri_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/gravitational/teleport/lib/teleterm/api/uri"
)

func TestURI(t *testing.T) {
	testCases := []struct {
		in  uri.ResourceURI
		out string
	}{
		{
			uri.Cluster("teleport.sh").Server("server1"),
			"/clusters/teleport.sh/servers/server1",
		},
		{
			uri.Cluster("teleport.sh").App("app1"),
			"/clusters/teleport.sh/apps/app1",
		},
		{
			uri.Cluster("teleport.sh").DB("dbhost1"),
			"/clusters/teleport.sh/dbs/dbhost1",
		},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%v", tt.in), func(t *testing.T) {
			out := tt.in.String()
			if !reflect.DeepEqual(out, tt.out) {
				t.Errorf("out %#v, want %#v", out, tt.out)
			}
		})
	}
}
