package dbr

import (
	"context"
	"database/sql"
	"fmt"
	//"fmt"
)

// InsertStmt builds `INSERT INTO ...`.
type CaseUpdateStmt struct {
	runner
	EventReceiver
	Dialect
	Table        string
	PKey         string
	RunLen       int
	Column       []string
	Value        []CaseUpdateValue
	ReturnColumn []string
}
type CaseUpdateValue struct {
	Key string
	Val []interface{}
}
type CaseUpdateBuilder = CaseUpdateStmt

func (b *CaseUpdateStmt) Build(d Dialect, buf Buffer) error {

	if b.Table == "" {
		return ErrTableNotSpecified
	}

	if len(b.Column) == 0 {
		return ErrColumnNotSpecified
	}
	WhereKey := []string{}
	buf.WriteString("UPDATE ")
	buf.WriteString(d.QuoteIdent(b.Table))
	buf.WriteString(" SET ")
	for i, col := range b.Column {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(d.QuoteIdent(col))
		buf.WriteString(" = CASE ")
		buf.WriteString(d.QuoteIdent(b.PKey))
		for x, v := range b.Value {
			if x >= b.RunLen && b.RunLen > 0 {
				break
			}
			buf.WriteString(" WHEN ? THEN ? ")
			buf.WriteValue(v.Key)
			buf.WriteValue(v.Val[i])
		}
		buf.WriteString(" END ")
	}
	for x, v := range b.Value {
		if x >= b.RunLen && b.RunLen > 0 {
			break
		}
		WhereKey = append(WhereKey, v.Key)
	}
	//长度为10,Len为5
	if len(b.Value) > b.RunLen && b.RunLen != 0 {
		b.Value = b.Value[b.RunLen:]
	} else {
		b.Value = []CaseUpdateValue{}
	}

	buf.WriteString(" WHERE ")
	buf.WriteString(d.QuoteIdent(b.PKey))
	buf.WriteString(" IN (")
	i := 0
	for _, key := range WhereKey {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(" ? ")
		buf.WriteValue(key)
		i++
	}
	buf.WriteString(" )")
	return nil
}

// InsertInto creates an InsertStmt.
func CaseUpdate(table string) *CaseUpdateStmt {
	return &CaseUpdateStmt{
		Table: table,
		Value: []CaseUpdateValue{},
	}
}

// InsertInto creates an InsertStmt.
func (sess *Session) CaseUpdate(table string) *CaseUpdateStmt {
	b := CaseUpdate(table)
	b.runner = sess
	b.EventReceiver = sess.EventReceiver
	b.Dialect = sess.Dialect
	return b
}

// InsertInto creates an InsertStmt.
func (tx *Tx) CaseUpdate(table string) *CaseUpdateStmt {
	b := CaseUpdate(table)
	b.runner = tx
	b.EventReceiver = tx.EventReceiver
	b.Dialect = tx.Dialect
	return b
}

func (b *CaseUpdateStmt) Columns(column ...string) *CaseUpdateStmt {
	b.Column = column
	return b
}

// Values adds a tuple to be inserted.
// The order of the tuple should match Columns.
func (b *CaseUpdateStmt) Values(PKey interface{}, value ...interface{}) *CaseUpdateStmt {
	pk := fmt.Sprint(PKey)
	for k, v := range b.Value {
		if v.Key == pk {
			b.Value[k].Val = value
		}
	}
	b.Value = append(b.Value, CaseUpdateValue{
		Key: fmt.Sprint(PKey),
		Val: value,
	})
	//b.Value[fmt.Sprint(PKey)]=value
	return b
}

// Returning specifies the returning columns for postgres.
func (b *CaseUpdateStmt) Returning(column ...string) *CaseUpdateStmt {
	b.ReturnColumn = column
	return b
}

// Returning specifies the returning columns for postgres.
func (b *CaseUpdateStmt) SetRunLen(i int) *CaseUpdateStmt {
	//b.runnum
	b.RunLen = i
	return b
}
func (b *CaseUpdateStmt) Exec() error {
	var err error
	for len(b.Value) > 0 && err == nil {
		_, err = b.ExecContext(context.Background())
	}
	return err
}

func (b *CaseUpdateStmt) ExecContext(ctx context.Context) (sql.Result, error) {
	result, err := exec(ctx, b.runner, b.EventReceiver, b, b.Dialect)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (b *CaseUpdateStmt) LoadContext(ctx context.Context, value interface{}) error {
	_, err := query(ctx, b.runner, b.EventReceiver, b, b.Dialect, value)
	return err
}

func (b *CaseUpdateStmt) Load(value interface{}) error {
	return b.LoadContext(context.Background(), value)
}
