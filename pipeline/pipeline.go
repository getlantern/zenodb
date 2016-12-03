package pipeline

import (
	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
	"sync"
	"time"
)

type Field struct {
	Expr expr.Expr
	Name string
}

type Fields []Field

type Vals []encoding.Sequence

type OnRow func(key bytemap.ByteMap, vals Vals)

type Source interface {
	Fields() Fields

	Resolution() time.Duration

	AsOf() time.Time

	Until() time.Time

	Iterate(onRow OnRow) error
}

type Join struct {
	sources []Source
}

func (j *Join) Connect(source Source) {
	j.sources = append(j.sources, source)
}

func (j *Join) iterateSerial(onRow OnRow) error {
	for _, source := range j.sources {
		err := source.Iterate(onRow)
		if err != nil {
			return err
		}
	}

	return nil
}

func (j *Join) iterateParallel(onRow OnRow) error {
	if len(j.sources) == 1 {
		return j.iterateSerial(onRow)
	}

	var mx sync.Mutex
	errors := make(chan error, len(j.sources))

	for _, s := range j.sources {
		source := s
		go func() {
			errors <- source.Iterate(func(key bytemap.ByteMap, vals Vals) {
				mx.Lock()
				onRow(key, vals)
				mx.Unlock()
			})
		}()
	}

	// TODO: add timeout handling
	var finalErr error
	for range j.sources {
		err := <-errors
		if err != nil {
			finalErr = err
		}
	}

	return finalErr
}
