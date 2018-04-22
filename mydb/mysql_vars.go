package mydb

import (
	//"encoding/binary"
	"bytes"
	"dannytools/ehand"
	"database/sql"
	"fmt"
	"io/ioutil"
	"strconv"

	"strings"

	kitsFile "github.com/toolkits/file"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlVars struct {
	V_read_only bool
	V_pid_file  string
}

func MysqlShowGlobalVars(db *sql.DB) (MysqlVars, error) {
	var (
		err     error
		val     MysqlVars = MysqlVars{}
		varName string
		varVal  sql.RawBytes
	)

	rows, err := db.Query(C_mysql_sql_global_vars)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return val, ehand.WithStackError(err)
	}

	for rows.Next() {
		err = rows.Scan(&varName, &varVal)
		switch varName {
		case "read_only":
			tmpVal := string(varVal)

			tmpVal = strings.ToUpper(tmpVal)
			if tmpVal == "ON" {
				val.V_read_only = true
			} else {
				val.V_read_only = false
			}

		case "pid_file":
			tmpVal := string(varVal)
			val.V_pid_file = tmpVal

		}
	}

	return val, nil

}

func GetPidOfMysql(pidFile string) (int32, error) {
	if pidFile == "" {
		return 0, ehand.WithStackError(fmt.Errorf("parameter pidFile is empty"))
	}
	if !kitsFile.IsFile(pidFile) {
		return 0, ehand.WithStackError(fmt.Errorf("%s is not a file", pidFile))
	}

	b, err := ioutil.ReadFile(pidFile)
	if err != nil {
		return 0, ehand.WithStackError(fmt.Errorf("error to pidfile %s: %s", pidFile, err))
	}
	b = bytes.TrimSpace(b)
	tmpInt, err := strconv.ParseInt(string(b), 10, 32)
	if err != nil {
		return 0, ehand.WithStackError(err)
	} else {
		return int32(tmpInt), nil
	}

}
