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

	"github.com/stretchr/testify/require"

	"github.com/gravitational/teleport/lib/teleterm/api/uri"
)

func TestURLPath(t *testing.T) {
	testCases := []struct {
		in  string
		out uri.Path
	}{
		{
			"foo",
			uri.Path{Segments: []uri.Segment{
				{Const: "foo"},
			}},
		},

		{
			"/foo",
			uri.Path{Segments: []uri.Segment{
				{Const: ""},
				{Const: "foo"},
			}},
		},

		{
			":foo",
			uri.Path{Segments: []uri.Segment{
				{IsParam: true, Param: "foo"},
			}},
		},

		{
			"/:foo",
			uri.Path{Segments: []uri.Segment{
				{Const: ""},
				{IsParam: true, Param: "foo"},
			}},
		},

		{
			"foo/:bar",
			uri.Path{Segments: []uri.Segment{
				{Const: "foo"},
				{IsParam: true, Param: "bar"},
			}},
		},

		{
			"foo/:foo/bar/:bar",
			uri.Path{Segments: []uri.Segment{
				{Const: "foo"},
				{IsParam: true, Param: "foo"},
				{Const: "bar"},
				{IsParam: true, Param: "bar"},
			}},
		},

		{
			"foo/:bar/:baz/*",
			uri.Path{Trailing: true, Segments: []uri.Segment{
				{Const: "foo"},
				{IsParam: true, Param: "bar"},
				{IsParam: true, Param: "baz"},
			}},
		},

		{
			"/:/*",
			uri.Path{Trailing: true, Segments: []uri.Segment{
				{Const: ""},
				{IsParam: true, Param: ""},
			}},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.in, func(t *testing.T) {
			out := uri.NewPath(tt.in)

			if !reflect.DeepEqual(out, tt.out) {
				t.Errorf("out %#v, want %#v", out, tt.out)
			}
		})
	}
}

func TestMatch(t *testing.T) {
	testCases := []struct {
		Path string
		in   string
		out  uri.Match
		ok   bool
	}{
		{
			"foo",
			"foo",
			uri.Match{Params: map[string]string{}, Trailing: ""},
			true,
		},

		{
			"foo",
			"bar",
			uri.Match{},
			false,
		},

		{
			":foo",
			"bar",
			uri.Match{Params: map[string]string{"foo": "bar"}, Trailing: ""},
			true,
		},

		{
			"/:foo",
			"/bar",
			uri.Match{Params: map[string]string{"foo": "bar"}, Trailing: ""},
			true,
		},

		{
			"/:foo/bar/:baz",
			"/foo/bar/baz",
			uri.Match{Params: map[string]string{"foo": "foo", "baz": "baz"}, Trailing: ""},
			true,
		},

		{
			"/:foo/bar/:baz",
			"/foo/bax/baz",
			uri.Match{},
			false,
		},
		{
			"/:foo/:bar/:baz",
			"/foo/bar/baz",
			uri.Match{Params: map[string]string{"foo": "foo", "bar": "bar", "baz": "baz"}, Trailing: ""},
			true,
		},
		{
			"/:foo/:bar/:baz",
			"///",
			uri.Match{Params: map[string]string{"foo": "", "bar": "", "baz": ""}, Trailing: ""},
			true,
		},

		{
			"/:foo/:bar/:baz",
			"",
			uri.Match{},
			false,
		},

		{
			"/:foo/bar/:baz",
			"/foo/bax/baz/a/b/c",
			uri.Match{},
			false,
		},

		{
			"/:foo/bar/:baz",
			"/foo/bax/baz/",
			uri.Match{},
			false,
		},

		{
			"/:foo/bar/:baz/*",
			"/foo/bar/baz/a/b/c",
			uri.Match{Params: map[string]string{"foo": "foo", "baz": "baz"}, Trailing: "a/b/c"},
			true,
		},

		{
			"/:foo/bar/:baz/*",
			"/foo/bar/baz/",
			uri.Match{Params: map[string]string{"foo": "foo", "baz": "baz"}, Trailing: ""},
			true,
		},

		{
			"/:foo/bar/:baz/*",
			"/foo/bar/baz",
			uri.Match{},
			false,
		},

		{
			"/:foo/:bar/:baz/*",
			"////",
			uri.Match{Params: map[string]string{"foo": "", "bar": "", "baz": ""}, Trailing: ""},
			true,
		},

		{
			"/:foo/:bar/:baz/*",
			"/////",
			uri.Match{Params: map[string]string{"foo": "", "bar": "", "baz": ""}, Trailing: "/"},
			true,
		},

		{
			"*",
			"",
			uri.Match{Params: map[string]string{}, Trailing: ""},
			true,
		},

		{
			"/*",
			"",
			uri.Match{},
			false,
		},

		{
			"*",
			"/",
			uri.Match{Params: map[string]string{}, Trailing: "/"},
			true,
		},

		{
			"/*",
			"/",
			uri.Match{Params: map[string]string{}, Trailing: ""},
			true,
		},

		{
			"*",
			"/a/b/c",
			uri.Match{Params: map[string]string{}, Trailing: "/a/b/c"},
			true,
		},

		{
			"*",
			"a/b/c",
			uri.Match{Params: map[string]string{}, Trailing: "a/b/c"},
			true,
		},

		// Examples from documentation
		{
			"/shelves/:shelf/books/:book",
			"/shelves/foo/books/bar",
			uri.Match{Params: map[string]string{"shelf": "foo", "book": "bar"}},
			true,
		},
		{
			"/shelves/:shelf/books/:book",
			"/shelves/123/books/456",
			uri.Match{Params: map[string]string{"shelf": "123", "book": "456"}},
			true,
		},
		{
			"/shelves/:shelf/books/:book",
			"/shelves/123/books/",
			uri.Match{Params: map[string]string{"shelf": "123", "book": ""}},
			true,
		},
		{
			"/shelves/:shelf/books/:book",
			"/shelves//books/456",
			uri.Match{Params: map[string]string{"shelf": "", "book": "456"}},
			true,
		},
		{
			"/shelves/:shelf/books/:book",
			"/shelves//books/",
			uri.Match{Params: map[string]string{"shelf": "", "book": ""}},
			true,
		},
		{
			"/shelves/:shelf/books/:book",
			"/shelves/foo/books",
			uri.Match{},
			false,
		},
		{
			"/shelves/:shelf/books/:book",
			"/shelves/foo/books/bar/",
			uri.Match{},
			false,
		},
		{
			"/shelves/:shelf/books/:book",
			"/shelves/foo/books/pages/baz",
			uri.Match{},
			false,
		},
		{
			"/shelves/:shelf/books/:book",
			"/SHELVES/foo/books/bar",
			uri.Match{},
			false,
		},
		{
			"/shelves/:shelf/books/:book",
			"shelves/foo/books/bar",
			uri.Match{},
			false,
		},
		{
			"/users/:user/files/*",
			"/users/foo/files/",
			uri.Match{Params: map[string]string{"user": "foo"}, Trailing: ""},
			true,
		},
		{
			"/users/:user/files/*",
			"/users/foo/files/foo/bar/baz.txt",
			uri.Match{Params: map[string]string{"user": "foo"}, Trailing: "foo/bar/baz.txt"},
			true,
		},
		{
			"/users/:user/files/*",
			"/users/foo/files////",
			uri.Match{Params: map[string]string{"user": "foo"}, Trailing: "///"},
			true,
		},
		{
			"/users/:user/files/*",
			"/users/foo",
			uri.Match{},
			false,
		},
		{
			"/users/:user/files/*",
			"/users/foo/files",
			uri.Match{},
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%s/%s", tt.Path, tt.in), func(t *testing.T) {
			path := uri.NewPath(tt.Path)
			out, ok := path.Match(tt.in)

			if !reflect.DeepEqual(out, tt.out) {
				require.Fail(t, "out %#v, want %#v", out, tt.out)
			}

			if ok != tt.ok {
				require.Fail(t, "ok %#v, want %#v", ok, tt.ok)
			}

			// If no error was expected when matching the data, then we should be able
			// to round-trip back to the original data using Build.
			if tt.ok {
				if in, ok := path.Build(out); ok {
					if in != tt.in {
						require.Fail(t, "in %#v, want %#v", in, tt.in)
					}
				} else {
					require.Fail(t, "Build returned ok = false")

				}
			}
		})
	}
}
