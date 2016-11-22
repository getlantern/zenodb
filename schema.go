package zenodb

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/getlantern/yaml"
	"github.com/getlantern/zenodb/sql"
)

type Schema map[string]*TableOpts

func (db *DB) pollForSchema(filename string) error {
	stat, err := os.Stat(filename)
	if err != nil {
		return err
	}

	err = db.ApplySchemaFromFile(filename)
	if err != nil {
		log.Error(err)
		return err
	}

	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			newStat, err := os.Stat(filename)
			if err != nil {
				log.Errorf("Unable to stat schema: %v", err)
				continue
			}
			if newStat.ModTime().After(stat.ModTime()) || newStat.Size() != stat.Size() {
				log.Debug("Schema file changed, applying")
				applyErr := db.ApplySchemaFromFile(filename)
				if applyErr != nil {
					log.Error(applyErr)
				}
				stat = newStat
			}
		}
	}()

	return nil
}

func (db *DB) ApplySchemaFromFile(filename string) error {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	var schema Schema
	err = yaml.Unmarshal(b, &schema)
	if err != nil {
		log.Errorf("Error applying schema: %v", err)
		log.Debug(string(b))
		return err
	}
	return db.ApplySchema(schema)
}

func (db *DB) ApplySchema(_schema Schema) error {
	schema := make(Schema, len(_schema))
	// Convert all names in schema to lowercase
	for name, opts := range _schema {
		opts.Name = strings.ToLower(name)
		schema[opts.Name] = opts
	}

	// Identify dependencies
	var tables []*TableOpts
	for name, opts := range schema {
		if !opts.View {
			tables = append(tables, opts)
		} else {
			dependsOn, err := sql.TableFor(opts.SQL)
			if err != nil {
				return fmt.Errorf("Unable to determine underlying table for view %v: %v", name, err)
			}
			table, found := schema[dependsOn]
			if !found {
				return fmt.Errorf("Table %v needed by view %v not found", name, dependsOn)
			}
			table.dependencyOf = append(table.dependencyOf, opts)
		}
	}
	// Apply tables in order of dependencies
	bd := &byDependency{}
	for _, opts := range tables {
		bd.add(opts)
	}
	log.Debugf("Applying tables in order: %v", strings.Join(bd.names, ", "))
	for _, opts := range bd.opts {
		name := opts.Name
		t := db.getTable(name)
		if t == nil {
			tableType := "table"
			create := db.CreateTable
			if opts.View {
				tableType = "view"
				create = db.CreateView
			}
			log.Debugf("Creating %v '%v' as\n%v", tableType, name, opts.SQL)
			log.Debugf("MaxFlushLatency: %v    MinFlushLatency: %v", opts.MaxFlushLatency, opts.MinFlushLatency)
			err := create(opts)
			if err != nil {
				return fmt.Errorf("Error creating table %v: %v", name, err)
			}
			log.Debugf("Created %v %v", tableType, name)
		} else {
			// TODO: support more comprehensive altering of tables (maybe)
			q, err := sql.Parse(opts.SQL, nil)
			if err != nil {
				return err
			}
			log.Debugf("Cowardly altering where and nothing else on table '%v': %v", name, q.Where)
			t.applyWhere(q.Where)
		}
	}

	return nil
}

type byDependency struct {
	opts  []*TableOpts
	names []string
}

func (bd *byDependency) add(opts *TableOpts) {
	bd.opts = append(bd.opts, opts)
	bd.names = append(bd.names, opts.Name)
	for _, dep := range opts.dependencyOf {
		bd.add(dep)
	}
}
