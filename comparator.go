// test
package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	"github.com/satori/go.uuid"
)

const configFile = "config.json"
const debuglevel = "INFO"

var logger = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)

func logMsg(level string, message string) {
	switch level {
	case "ERROR":
		{
			if debuglevel == "INFO" || debuglevel == "DEBUG" || debuglevel == "WARNING" || debuglevel == "ERROR" {
				logger.Println("ERROR: " + message)
			}
		}
	case "WARNING":
		{
			if debuglevel == "INFO" || debuglevel == "DEBUG" || debuglevel == "WARNING" {
				logger.Println("DEBUG: " + message)
			}
		}
	case "DEBUG":
		{
			if debuglevel == "INFO" || debuglevel == "DEBUG" {
				logger.Println("DEBUG: " + message)
			}
		}
	case "INFO":
		{
			if debuglevel == "INFO" {
				logger.Println("INFO: " + message)
			}
		}
	default:
		{

		}
	}
}

func getMD5Result(r *sql.Rows) [16]byte {
	var result = ""
	for r.Next() {
		var name string
		var count int
		r.Scan(&name, &count)
		result += name + ":" + strconv.Itoa(count) + "|"
	}
	return md5.Sum([]byte(result))
}

func getMD5FromRequest(field string, db *sql.DB, uid string) (res [16]byte, error bool) {
	var md5Field [16]byte
	var lastPoint = strings.LastIndex(field, ".")
	if lastPoint > 0 {
		var tablename = field[0:lastPoint]
		logMsg("INFO", "UUID: "+uid+" tablename="+tablename)
		var query = "select " + field + ", count(*) from " + tablename + " group by " + field + " order by " + field
		logMsg("INFO", "UUID: "+uid+" Exec request: "+query)
		rows, err := db.Query(query)
		if err != nil {
			logMsg("ERROR", "UUID: "+uid+" Error while executing request: "+query)
			return md5Field, true
		}
		defer rows.Close()
		md5Field = getMD5Result(rows)
		MD5String := hex.EncodeToString(md5Field[:])
		logMsg("INFO", "UUID: "+uid+" md5 of result: "+MD5String)
		return md5Field, false
	} else {
		return md5Field, true
	}
}

func compareRows(field1 string, field2 string, db1 *sql.DB, db2 *sql.DB, c chan bool, uid string) {
	var md5Field1 [16]byte
	var md5Field2 [16]byte
	if field1 == "" || field2 == "" {
		c <- false
		return
	} else {
		var error bool
		md5Field1, error = getMD5FromRequest(field1, db1, uid)
		if error {
			c <- false
			return
		}
		md5Field2, error = getMD5FromRequest(field2, db2, uid)
		if error {
			c <- false
			return
		}
	}

	if md5Field1 == md5Field2 {
		logMsg("ERROR", "UUID: "+uid+" "+field1+" uncorrectly exported. Differences detected")
	} else {
		logMsg("ERROR", "UUID: "+uid+" "+field1+" correctly exported. No Difference detected")
	}
	c <- true
}

func createDBConnection(drivername string, user string, password string, databasename string, host string, portnumber string) (*sql.DB, error) {
	db, err := sql.Open(drivername, "user="+user+" password="+password+" dbname="+databasename+" host="+host+" port="+portnumber)
	if err != nil {
		logMsg("ERROR", "Error while validating arguments of the connection to the first database. Err="+err.Error())
		return nil, err
	}
	// Open may just validate its arguments without creating a connection to the database. To verify that the data source name is valid, call Ping
	err = db.Ping()
	if err != nil {
		logMsg("ERROR", "Error while testing connection to the first database. Err="+err.Error())
		return nil, err
	}
	return db, err
}

func main() {
	logMsg("INFO", "Config file = "+configFile)
	r, e := ioutil.ReadFile(configFile)
	if e != nil {
		logMsg("ERROR", "Impossible de lire le fichier de configuration. Err:"+e.Error())
	} else {
		logMsg("INFO", "Config = "+string(r[:]))
	}

	type database_config struct {
		Host         string
		Port         int
		Login        string
		Password     string
		DatabaseName string
	}

	type field_to_compare struct {
		Field1 string
		Field2 string
	}

	type json_config_file struct {
		Databases      []database_config  `json:"Databases"`
		FieldToCompare []field_to_compare `json:"FieldToCompare"`
	}

	// Retrieve database configuration
	var json_config json_config_file
	err := json.Unmarshal(r, &json_config)
	if err != nil {
		logMsg("INFO", "Error while unmarshalling json config file:"+err.Error())
	}

	// Open connexion to DB 1
	portNumber := strconv.Itoa(json_config.Databases[0].Port)
	db1, err := createDBConnection("postgres", json_config.Databases[0].Login, json_config.Databases[0].Password, json_config.Databases[0].DatabaseName, json_config.Databases[0].Host, portNumber)
	if err != nil {
		os.Exit(0)
	}
	defer db1.Close()

	// Open connexion to DB 2
	portNumber = strconv.Itoa(json_config.Databases[1].Port)
	db2, err := createDBConnection("postgres", json_config.Databases[1].Login, json_config.Databases[1].Password, json_config.Databases[1].DatabaseName, json_config.Databases[1].Host, portNumber)
	if err != nil {
		os.Exit(0)
	}
	defer db2.Close()

	rows, err := db1.Query("select count(*) from Person.Person")
	if err != nil {
		logMsg("INFO", "Error while executing request: select count(*) from Person.Person")
		os.Exit(0)
	}

	for rows.Next() {
		var count int
		err = rows.Scan(&count)
		logMsg("INFO", "Results: "+strconv.Itoa(count))
	}
	rows.Close()

	var c = make(chan bool)
	for i := 0; i < len(json_config.FieldToCompare); i++ {
		routineuuid := uuid.NewV4().String()
		go compareRows(json_config.FieldToCompare[i].Field1, json_config.FieldToCompare[i].Field2, db1, db2, c, routineuuid)
	}
	for i := 0; i < len(json_config.FieldToCompare); i++ {
		<-c
	}
}
