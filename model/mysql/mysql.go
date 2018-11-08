package mysql

import (
	"database/sql"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var (
	db      *sql.DB
	dbMutex sync.Mutex
)

// ---------------------------------------------------------------------------------------------------------------------

// init mysql
func Init(dataSource string) error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	var err error
	db, err = sql.Open("mysql", dataSource)
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(1000)
	db.SetMaxIdleConns(200)

	if err := db.Ping(); err != nil {
		db.Close()
		return err
	}

	return nil
}

func FreeDB() {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	if db != nil {
		db.Close()
	}
}
