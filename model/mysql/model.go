package mysql

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"
)

// 模型：每个业务模型都需要继承该模型
type Model struct {
	TableName string  `json:"-"`
	Tx        *sql.Tx `json:"-"`
}

// 获取DB
func (this *Model) GetDB() *sql.DB {
	return db
}

// 构建并获取查询语句
func (this *Model) Select(fields string) *Query {
	return &Query{
		Sql: fmt.Sprintf("SELECT %s", fields),
	}
}

// 基于SQL查询
func (this *Model) SelectBySql(sql string, value ...interface{}) (*sql.Rows, error) {
	if this.Tx == nil {
		return db.Query(sql, value)
	} else {
		return this.Tx.Query(sql, value)
	}
}

// 查询单条
func (this *Model) SelectWhere(query *Query, exps interface{}) (*sql.Row, error) {
	if query == nil {
		return nil, fmt.Errorf("params error")
	}

	retWhere, err := this.getWhereByInterface(exps)
	if err != nil {
		return nil, err
	}

	sql := fmt.Sprintf("%s %s", query.Sql, retWhere)

	if this.Tx == nil {
		fmt.Println("db sql: ", sql)
		return db.QueryRow(sql), nil
	} else {
		return this.Tx.QueryRow(sql), nil
	}
}

// 查询多条
func (this *Model) SelectRowsWhere(query *Query, exps interface{}) (*sql.Rows, error) {
	if query == nil {
		return nil, fmt.Errorf("params error")
	}

	retWhere, err := this.getWhereByInterface(exps)
	if err != nil {
		return nil, err
	}

	sql := fmt.Sprintf("%s %s", query.Sql, retWhere)

	if this.Tx == nil {
		return db.Query(sql)
	} else {
		return this.Tx.Query(sql)
	}
}

// 插入params数据
func (this *Model) Insert(params map[string]interface{}) (int64, error) {
	var err error
	var result sql.Result

	length := len(params)
	columns := make([]string, 0, length)
	values := make([]string, 0, length)
	for key, value := range params {
		columns = append(columns, key)
		values = append(values, fmt.Sprintf("%v", value))
	}

	fields := fmt.Sprintf("`%s`", strings.Join(columns, "`,`"))
	fieldValues := fmt.Sprintf("'%s'", strings.Join(values, "','"))
	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", this.TableName, fields, fieldValues)

	if this.Tx == nil {
		if result, err = db.Exec(sql); err != nil {
			return 0, err
		}
	} else {
		if result, err = this.Tx.Exec(sql); err != nil {
			return 0, err
		}
	}

	return result.LastInsertId()
}

// 更新：基于exps表达式更新params数据
func (this *Model) Update(params map[string]interface{}, exps interface{}) (int64, error) {
	var result sql.Result

	retWhere, err := this.getWhereByInterface(exps)
	if err != nil {
		return 0, err
	}

	length := len(params)
	setValues := make([]string, 0, length)
	for key, value := range params {
		set := fmt.Sprintf("`%v`='%v'", key, value)
		setValues = append(setValues, set)
	}

	retSet := strings.Join(setValues, ", ")
	sql := fmt.Sprintf("UPDATE %s SET %s %s", this.TableName, retSet, retWhere)

	if this.Tx == nil {
		if result, err = db.Exec(sql); err != nil {
			return 0, err
		}
	} else {
		if result, err = this.Tx.Exec(sql); err != nil {
			return 0, err
		}
	}

	return result.RowsAffected()
}

// 删除：基于exps表达式删除数据
func (this *Model) Delete(exps interface{}) (int64, error) {
	var result sql.Result

	retWhere, err := this.getWhereByInterface(exps)
	if err != nil {
		return 0, err
	}

	sql := fmt.Sprintf("DELETE FROM %v %v", this.TableName, retWhere)

	if this.Tx == nil {
		if result, err = db.Exec(sql); err != nil {
			return 0, err
		}
	} else {
		if result, err = this.Tx.Exec(sql); err != nil {
			return 0, err
		}
	}

	return result.RowsAffected()
}

func (this *Model) BatchInsert(columns []string, params []interface{}) (int64, error) {
	var err error
	var lastInsertId int64

	paramsLen := len(params)
	count, frac := math.Modf(float64(paramsLen) / max_bantch_limit)
	if frac > 0.000001 {
		count += 1
	}

	for i := 0; i < int(count); i++ {
		var endIndex int
		if (i+1)*max_bantch_limit > paramsLen {
			endIndex = paramsLen
		} else {
			endIndex = (i + 1) * max_bantch_limit
		}

		lastInsertId, err = this.batchInsertByLimit(columns, params[i*max_bantch_limit:endIndex])
		if err != nil {
			return 0, err
		}
	}

	return lastInsertId, err
}

// ---------------------------------------------------------------------------------------------------------------------

// 基于表达式获取并构建where语句
func (this *Model) getWhereByInterface(exps interface{}) (string, error) {
	var result string

	switch exps.(type) {
	case map[string]interface{}:
		result = fmt.Sprintf("WHERE %s", this.getWhereIterm("AND", exps.(map[string]interface{})))

	case map[string]map[string]interface{}:
		length := len(exps.(map[string]map[string]interface{}))
		if length > 0 {
			wheres := make([]string, 0, length)
			for key, value := range exps.(map[string]map[string]interface{}) {
				keyToUpper := strings.ToUpper(key)
				if keyToUpper == "AND" || keyToUpper == "OR" {
					wheres = append(wheres, this.getWhereIterm(keyToUpper, value))
				} else {
					return "", fmt.Errorf("params error")
				}
			}
			result = fmt.Sprintf("WHERE %s", strings.Join(wheres, " AND "))
		}

	default:
		return "", fmt.Errorf("params error")
	}

	return result, nil
}

// 获取并构建where中的每个子项
func (this *Model) getWhereIterm(join string, exps map[string]interface{}) string {
	var result string

	if length := len(exps); length > 0 {
		where := make([]string, 0, length)
		for key, value := range exps {
			where = append(where, strings.Replace(key, "?", fmt.Sprintf("'%v'", value), -1))
		}
		result = fmt.Sprintf("(%s)", strings.Join(where, fmt.Sprintf(" %s ", join)))
	}

	return result
}

func (this *Model) batchInsertByLimit(columns []string, params []interface{}) (int64, error) {
	paramsLen := len(params)
	if paramsLen > max_bantch_limit {
		return 0, fmt.Errorf("batch insert too large, length: %v", paramsLen)
	}

	data := make([]string, paramsLen)
	for i, v := range params {
		val := reflect.ValueOf(v)
		if val.Kind() != reflect.Slice {
			return 0, fmt.Errorf("params error, insert data must be slice")
		}

		var subVal string
		subData := make([]string, 0, val.Len())

		for j := 0; j < val.Len(); j++ {
			switch t := val.Index(j).Interface().(type) {
			case string:
				subVal = fmt.Sprintf("'%s'", t)
			case sql.NullString:
				subVal = fmt.Sprintf("'%s'", t.String)
			default:
				subVal = fmt.Sprintf("%v", t)
			}
			subData = append(subData, subVal)
		}
		data[i] = fmt.Sprintf("(%s)", strings.Join(subData, ","))
	}

	var err error
	var result sql.Result
	cmd := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		this.TableName, strings.Join(columns, ","), strings.Join(data, ","))

	if this.Tx == nil {
		if result, err = db.Exec(cmd); err != nil {
			return 0, err
		}
	} else {
		if result, err = this.Tx.Exec(cmd); err != nil {
			return 0, err
		}
	}

	return result.LastInsertId()
}
