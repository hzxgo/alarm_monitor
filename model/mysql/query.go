package mysql

import "fmt"

type Query struct {
	Sql string `json:"-"`
}

func (this *Query) Form(tableName string) *Query {
	this.Sql = fmt.Sprintf("%s FROM %s", this.Sql, tableName)
	return this
}

func (this *Query) LeftJoin(tableName, on string) *Query {
	this.Sql = fmt.Sprintf("%s LEFT JOIN (%s) ON (%s)", this.Sql, tableName, on)
	return this
}

func (this *Query) OrderBy(field string) *Query {
	this.Sql = fmt.Sprintf("%s ORDER BY %s", this.Sql, field)
	return this
}

func (this *Query) OrderAsc(field string) *Query {
	this.Sql = fmt.Sprintf("%s ORDER BY %s ASC", this.Sql, field)
	return this
}

func (this *Query) OrderDesc(field string) *Query {
	this.Sql = fmt.Sprintf("%s ORDER BY %s DESC", this.Sql, field)
	return this
}

func (this *Query) Limit(limit uint64) *Query {
	this.Sql = fmt.Sprintf("%s LIMIT %d", this.Sql, limit)
	return this
}
