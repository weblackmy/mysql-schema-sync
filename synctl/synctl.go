package synctl

import (
	"flag"
	"log"
	"os"
	"reflect"
	"strings"
	"time"
)

type SyncCtl struct {
	SourceSchema *MySchema
	TargetSchema *MySchema
	tablesAdd    []*MyTable
	tablesDrop   []*MyTable
	tablesChange []*fieldAction
}

type execResult struct {
	query string
	err   error
	time  time.Duration
}

type fieldAction struct {
	field  *MyField
	action string //add, drop, change
}

func init() {
	// set parameters
	flag.StringVar(&sourceDsn, "sourceDsn", "", "A valid mysql dsn")
	flag.StringVar(&targetDsn, "targetDsn", "", "A valid mysql dsn")
}

var (
	sourceDsn string
	targetDsn string
	flog      *log.Logger
)

func Start() {
	flag.Parse()
	ctl := &SyncCtl{}
	flog = log.New(os.Stdout, "", log.LstdFlags)
	defer func() {
		if err := recover(); err != nil {
			flog.Fatalf("Exit with error: %s", err)
		}
	}()

	if sourceDsn == "" || targetDsn == "" {
		panic("sourceDsn or targetDsn is empty")
	}

	//for test start
	//sourceDsn := "root:123456@(127.0.0.1:3306)/test_source" //源库, 需要被比对的库
	//targetDsn := "root:123456@(127.0.0.1:3306)/test_target" //目标库
	//for test end

	ctl.SourceSchema = NewMySchema(sourceDsn)
	ctl.TargetSchema = NewMySchema(targetDsn)

	// step 1
	ctl.structure()
}

// table structure
func (ctl *SyncCtl) structure() {
	ctl.compareTables()
	if len(ctl.tablesAdd) == 0 || len(ctl.tablesDrop) == 0 || len(ctl.tablesChange) == 0 {
		log.Printf("Database %s h1as already synced\n", ctl.TargetSchema.dbName)
		os.Exit(0)
	}

	var (
		res execResult
		err error
	)

	for _, table := range ctl.tablesAdd {
		if res, err = table.create(); err != nil {
			panic(err)
		}
		showRes(res)
	}

	for _, table := range ctl.tablesDrop {
		if res, err = table.drop(); err != nil {
			panic(err)
		}
		showRes(res)
	}

	for _, fa := range ctl.tablesChange {
		switch fa.action {
		case "add":
			res, err = fa.field.add()
		case "drop":
			res, err = fa.field.drop()
		case "change":
			res, err = fa.field.change()
		}
		if err != nil {
			panic(err)
		}
		showRes(res)
	}
}

// to determine which table add, delete, change
func (ctl *SyncCtl) compareTables() {
	for _, val := range ctl.SourceSchema.Tables {
		sourceTable, err := NewMyTable(val, ctl.SourceSchema, true)
		if err != nil {
			continue
		}
		// may be table not exist in target schema
		targetTable, _ := NewMyTable(val, ctl.TargetSchema, false)
		if inStringSlice(sourceTable.Name, &ctl.TargetSchema.Tables) {
			targetTable.initTable()
			if sourceTable.RawShowCreateTable == targetTable.RawShowCreateTable {
				// table not changed
				continue
			} else if reflect.DeepEqual(sourceTable.Fields, targetTable.Fields) {
				// fields not change
				continue
			} else {
				//change
				ctl.compareFields(sourceTable, targetTable)
			}
		} else {
			targetTable.RawShowCreateTable = sourceTable.RawShowCreateTable
			ctl.tablesAdd = append(ctl.tablesAdd, targetTable)
		}
	}

	// for table has delete in source schema
	for _, val := range ctl.TargetSchema.Tables {
		if !inStringSlice(val, &ctl.SourceSchema.Tables) {
			targetTable, err := NewMyTable(val, ctl.TargetSchema, true)
			if err != nil {
				continue
			}
			ctl.tablesDrop = append(ctl.tablesDrop, targetTable)
		}
	}
}

// to determine which field add, delete, change
func (ctl *SyncCtl) compareFields(sourceTable *MyTable, targetTable *MyTable) {
	for field, sVal := range sourceTable.Fields {
		cf := &fieldAction{
			field: NewMyField(field, targetTable),
		}
		cf.field.rawQuery = sVal
		if tVal, ok := targetTable.Fields[field]; ok {
			if sVal == tVal { // field not change
				continue
			} else {
				cf.action = "change"
			}
		} else { // field not exist
			cf.action = "add"
		}
		ctl.tablesChange = append(ctl.tablesChange, cf)
	}

	// for field has delete in source table
	for field, _ := range targetTable.Fields {
		cf := &fieldAction{
			field:  NewMyField(field, targetTable),
			action: "drop",
		}
		if _, ok := sourceTable.Fields[field]; !ok {
			ctl.tablesChange = append(ctl.tablesChange, cf)
		}
	}
}

func showRes(res execResult) {
	query := res.query
	if strings.Contains(res.query, "CREATE TABLE") {
		query = strings.Join(strings.Split(res.query, "\n"), "")
	}
	flog.Printf("exec query successfully, time:%dms, query:%s\n", res.time, query)
}

func inStringSlice(str string, strSlice *[]string) bool {
	for _, val := range *strSlice {
		if str == val {
			return true
		}
	}
	return false
}
