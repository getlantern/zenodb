package tdb

import (
	"fmt"

	"github.com/getlantern/tdb/sql"
)

type sortedFields []sql.Field

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
