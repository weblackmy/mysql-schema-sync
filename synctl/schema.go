package synctl

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"regexp"
	"strings"
	"time"
)

type MySchema struct {
	db     *sql.DB
	Tables []string
}

type MyTable struct {
	Name               string
	Fields             map[string]string
	RawShowCreateTable string
	Schema             *MySchema
}

type MyField struct {
	Name     string
	Table    *MyTable
	rawQuery string
}

func NewMySchema(dsn string) *MySchema {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic("open database failed with error:" + err.Error())
	}
	schema := &MySchema{
		db: db,
	}
	if err = schema.initSchema(); err != nil {
		panic(err)
	}
	return schema
}

func NewMyTable(name string, schema *MySchema, initTable bool) (*MyTable, error) {
	table := &MyTable{
		Name:   name,
		Schema: schema,
	}

	if initTable {
		err := table.initTable()
		if err != nil {
			flog.Printf("init table %s failed", name)
			return nil, err
		}
	}
	return table, nil
}

func NewMyField(name string, table *MyTable) *MyField {
	field := &MyField{
		Name:  name,
		Table: table,
	}
	return field
}

// init database tables
func (schema *MySchema) initSchema() error {
	var (
		err error
	)
	if schema.Tables, err = schema.GetTables(); err != nil {
		return err
	}
	return nil
}

// get tables
func (schema *MySchema) GetTables() ([]string, error) {
	var (
		rows    *sql.Rows
		tables  []string
		columns []string
		err     error
	)
	if rows, err = schema.db.Query("show table status"); err != nil {
		return nil, err
	}
	defer rows.Close()

	if columns, err = rows.Columns(); err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))
	// a temporary slice for scan
	scanArgs := make([]interface{}, len(columns))
	for i, _ := range scanArgs {
		scanArgs[i] = &values[i]
	}

	var fieldsValue = make(map[string]sql.RawBytes)
	for rows.Next() {
		rows.Scan(scanArgs...)
		for i, value := range values {
			// value is null
			//if value == nil {
			//   fieldsValue[columns[i]] = "NULL"
			//} else {
			//   fieldsValue[columns[i]] = string(value)
			//}
			fieldsValue[columns[i]] = value
		}
		if v, ok := fieldsValue["Name"]; ok {
			tables = append(tables, string(v))
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

// init table
func (table *MyTable) initTable() error {
	var (
		err error
	)
	if table.RawShowCreateTable, err = table.showCreateTable(); err != nil {
		return err
	}
	table.Fields = table.GetFields()
	return nil
}

// get table fields, exclude index
func (table *MyTable) GetFields() map[string]string {
	fields := make(map[string]string)
	re := regexp.MustCompile(`\w+`)
	tableSchemaLines := strings.Split(strings.TrimSpace(table.RawShowCreateTable), "\n")
	for i, line := range tableSchemaLines {
		if i == 0 || i == len(tableSchemaLines)-1 {
			continue
		}
		//if first letter is "`"
		line = strings.TrimRight(strings.TrimSpace(line), ",")
		if strings.HasPrefix(line, "`") {
			if s := re.FindStringSubmatch(line); len(s) > 0 {
				fields[s[0]] = line
			}
		}
	}
	return fields
}

// query `show create table`
func (table *MyTable) showCreateTable() (string, error) {
	var (
		tableName   string
		tableSchema string
		err         error
	)
	if err = table.Schema.db.QueryRow("show create table `"+table.Name+"`").Scan(&tableName, &tableSchema); err != nil {
		return "", err
	}
	return tableSchema, nil
}

func (table *MyTable) create(ch chan execResult) {
	table.exec(ch, table.RawShowCreateTable)
}

func (table *MyTable) drop(ch chan execResult) {
	table.exec(ch, "DROP TABLE `"+table.Name+"`")
}

func (field *MyField) add(ch chan execResult) {
	field.Table.exec(ch, "ALTER TABLE `"+field.Table.Name+"` ADD "+field.rawQuery+";")
}

func (field *MyField) drop(ch chan execResult) {
	field.Table.exec(ch, "ALTER TABLE `"+field.Table.Name+"` DROP `"+field.Name+"`")
}

func (field *MyField) change(ch chan execResult) {
	field.drop(ch)
	field.add(ch)
}

func (table *MyTable) exec(ch chan execResult, query string) {
	start := time.Now()
	_, err := table.Schema.db.Exec(query)
	if err != nil {
		ch <- execResult{err: err}
	}
	end := time.Now()
	ch <- execResult{err: nil, query: query, time: end.Sub(start) / time.Millisecond}
}
