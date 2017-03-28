package core

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/getlantern/bytemap"
)

func Crosstab(source FlatRowSource) FlatRowSource {
	return &crosstabber{
		flatRowTransform{source},
	}
}

type crosstabber struct {
	flatRowTransform
}

func (c *crosstabber) Iterate(ctx context.Context, onFields OnFields, onRow OnFlatRow) error {
	var inFields Fields
	var inRows crosstabRows
	uniqueCTs := make(map[string]interface{})

	err := c.source.Iterate(ctx, func(fields Fields) error {
		inFields = fields
		return nil
	}, func(row *FlatRow) (bool, error) {
		_ct, key := row.Key.Split("_crosstab")
		ct := _ct.Get("_crosstab").(string)
		inRows = append(inRows, &crosstabRow{row, ct, key})
		uniqueCTs[ct] = nil
		return proceed()
	})

	if err != ErrDeadlineExceeded {
		deadline, hasDeadline := ctx.Deadline()

		// Build crosstab fields
		cts := make([]string, len(uniqueCTs))
		for ct := range uniqueCTs {
			cts = append(cts, ct)
		}
		sort.Strings(cts)
		outFields := make(Fields, 0, 1000)
		ctis := make(map[string]int, 1000)
		for cti, ct := range cts {
			ctis[ct] = cti
			if len(inFields) == 1 {
				// For single-field cross tabs, just use the crosstab name
				outFields = append(outFields, NewField(ct, inFields[0].Expr))
			} else {
				for _, field := range inFields {
					outFields = append(outFields, NewField(fmt.Sprintf("%v_%v", ct, field.Name), field.Expr))
				}
			}
		}

		// Let caller know about fields
		err = onFields(outFields)
		if err != nil {
			return err
		}

		// Sort input rows and build output rows
		sort.Sort(inRows)
		numFields := len(outFields)
		var priorKey bytemap.ByteMap
		var priorTS int64
		var currentRow *FlatRow
		for _, row := range inRows {
			if hasDeadline && time.Now().After(deadline) {
				return ErrDeadlineExceeded
			}
			if !bytes.Equal(row.key, priorKey) || priorTS > row.row.TS {
				if currentRow != nil {
					more, onRowErr := onRow(currentRow)
					if onRowErr != nil {
						return onRowErr
					}
					if !more {
						break
					}
				}
				currentRow = &FlatRow{
					TS:     row.row.TS,
					Key:    row.key,
					Values: make([]float64, numFields),
					fields: outFields,
				}
			}
			cti := ctis[row.ct]
			for i, value := range row.row.Values {
				fieldIdx := cti + i
				currentRow.Values[fieldIdx] = value
			}
		}
	}

	return err
}

func (c *crosstabber) String() string {
	return fmt.Sprintf("crosstab")
}

type crosstabRow struct {
	row *FlatRow
	ct  string
	key bytemap.ByteMap
}

type crosstabRows []*crosstabRow

func (r crosstabRows) Len() int      { return len(r) }
func (r crosstabRows) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r crosstabRows) Less(i, j int) bool {
	a, b := r[i], r[j]

	// Sort primarily by key
	kc := bytes.Compare(a.key, b.key)
	if kc < 0 {
		return true
	}
	if kc > 0 {
		return false
	}

	// Then sort by TS (descending)
	return a.row.TS > b.row.TS
}
