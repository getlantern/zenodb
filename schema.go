package tibsdb

import (
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/getlantern/tibsdb/sql"
	"github.com/getlantern/yaml"
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

func (db *DB) ApplySchema(schema Schema) error {
	for name, opts := range schema {
		name = strings.ToLower(name)
		opts.Name = name
		t := db.getTable(name)
		if t == nil {
			tableType := "table"
			create := db.CreateTable
			if opts.View {
				tableType = "view"
				create = db.CreateView
			}
			log.Debugf("Creating %v '%v' as\n%v", tableType, name, opts.SQL)
			err := create(opts)
			if err != nil {
				return err
			}
			log.Debugf("Created %v %v", tableType, name)
		} else {
			// TODO: support more comprehensive altering of tables (maybe)
			q, err := sql.Parse(opts.SQL)
			if err != nil {
				return err
			}
			log.Debugf("Cowardly altering where and nothing else on table '%v': %v", name, q.Where)
			t.applyWhere(q.Where)
		}
	}

	return nil
}
