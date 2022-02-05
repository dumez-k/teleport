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

// Package urlpath matches paths against a template. It's meant for applications
// that take in REST-like URL paths, and need to validate and extract data from
// those paths.
//
// See New for documentation of the syntax for creating paths. See Match for how
// to validate and parse an inputted path.
package uri

import (
	"fmt"

	"github.com/gravitational/trace"
)

var pathClusters = NewPath("/clusters/:cluster/*")
var pathLeafClusters = NewPath("/clusters/:cluster/leaves/:leaf/*")
var pathGateways = NewPath("/gateways/:gateway")

func New(path string) ResourceURI {
	return ResourceURI{
		Path: path,
	}
}

func NewCluster(name string) ResourceURI {
	return ResourceURI{
		Path: fmt.Sprintf("/clusters/%v", name),
	}
}

func NewGateway(id string) ResourceURI {
	return ResourceURI{
		Path: fmt.Sprintf("/gateways/%v", id),
	}
}

type ResourceURI struct {
	Path string
}

func (r ResourceURI) GetCluster() string {
	result, ok := pathClusters.Match(r.Path + "/")
	if !ok {
		return ""
	}

	return result.Params["cluster"]
}

func (r ResourceURI) GetLeafCluster() string {
	result, ok := pathLeafClusters.Match(r.Path + "/")
	if !ok {
		return ""
	}

	return result.Params["leaf"]
}

func (r ResourceURI) GetGateway() string {
	result, ok := pathGateways.Match(r.Path)
	if !ok {
		return ""
	}

	return result.Params["gateway"]
}

func (r ResourceURI) Server(id string) ResourceURI {
	r.Path = fmt.Sprintf("%v/servers/%v", r.Path, id)
	return r
}

func (r ResourceURI) Leaf(id string) ResourceURI {
	r.Path = fmt.Sprintf("%v/leaves/%v", r.Path, id)
	return r
}

func (r ResourceURI) Kube(id string) ResourceURI {
	r.Path = fmt.Sprintf("%v/kubes/%v", r.Path, id)
	return r
}

func (r ResourceURI) DB(id string) ResourceURI {
	r.Path = fmt.Sprintf("%v/dbs/%v", r.Path, id)
	return r
}

func (r ResourceURI) Gateway(id string) ResourceURI {
	r.Path = fmt.Sprintf("%v/gateways/%v", r.Path, id)
	return r
}

func (r ResourceURI) App(id string) ResourceURI {
	r.Path = fmt.Sprintf("%v/apps/%v", r.Path, id)
	return r
}

func (r ResourceURI) String() string {
	return r.Path
}

// NewClusterFromResource creates cluster URI based off resource URI
func NewClusterFromResource(resourceURI string) (ResourceURI, error) {
	URI := New(resourceURI)
	rootClusterName := URI.GetCluster()
	leafClusterName := URI.GetLeafCluster()

	if rootClusterName == "" {
		return URI, trace.BadParameter("missing root cluster name")
	}

	clusterURI := NewCluster(rootClusterName)
	if leafClusterName != "" {
		clusterURI = clusterURI.Leaf(leafClusterName)
	}

	return clusterURI, nil
}
