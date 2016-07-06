package tdb

import (
	"fmt"
	"sort"

	"github.com/getlantern/tdb/expr"
)

func sortFields(fields map[string]expr.Expr) sortedFields {
	if fields == nil {
		return make(sortedFields, 0)
	}
	sorted := make(sortedFields, len(fields))
	i := 0
	for name, expr := range fields {
		sorted[i].Name = name
		sorted[i].Expr = expr
		i++
	}
	sort.Sort(sorted)
	return sorted
}

type sortedFields []field

func (a sortedFields) Len() int      { return len(a) }
func (a sortedFields) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortedFields) Less(i, j int) bool {
	ai := a[i]
	aj := a[j]
	id := ai.DependsOn()
	jd := aj.DependsOn()
	iDependsOnJ := false
	jDependsOnI := false
	for _, fi := range id {
		if fi == aj.Name {
			iDependsOnJ = true
		}
	}
	for _, fj := range jd {
		if fj == ai.Name {
			jDependsOnI = true
		}
	}
	if iDependsOnJ {
		if jDependsOnI {
			panic(fmt.Sprintf("Circular dependency between %v and %v", ai.Name, aj.Name))
		}
		return true
	}
	if jDependsOnI {
		return false
	}
	return ai.Name < aj.Name
}
