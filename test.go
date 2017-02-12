// test
package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

const configFile = "config.json"

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

func main() {
	fmt.Println("Hello World!")
	fmt.Println("Config file = " + configFile)
	r, e := ioutil.ReadFile(configFile)
	if e != nil {
		fmt.Println("Impossible de lire le fichier de configuration. Err:" + e.Error())
	} else {
		fmt.Printf("Config = %s\n", r)
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
		fmt.Println("Error while unmarshalling json config file:", err)
	}
	fmt.Printf("%+v\n", json_config)

	// Open connexion to DB 1
	portNumber := strconv.Itoa(json_config.Databases[0].Port)
	db1, err := sql.Open("postgres", "user="+json_config.Databases[0].Login+" password="+json_config.Databases[0].Password+" dbname="+json_config.Databases[0].DatabaseName+" host="+json_config.Databases[0].Host+" port="+portNumber)
	if err != nil {
		fmt.Println("Error while connecting to the first database. Err=" + err.Error())
		os.Exit(0)
	}
	defer db1.Close()

	// Open connexion to DB 2
	portNumber = strconv.Itoa(json_config.Databases[1].Port)
	db2, err := sql.Open("postgres", "user="+json_config.Databases[1].Login+" password="+json_config.Databases[1].Password+" dbname="+json_config.Databases[1].DatabaseName+" host="+json_config.Databases[1].Host+" port="+portNumber)
	if err != nil {
		fmt.Println("Error while connecting to the second database. Err=" + err.Error())
		os.Exit(0)
	}
	defer db2.Close()

	rows, err := db1.Query("select count(*) from Person.Person")
	if err != nil {
		fmt.Println("Error while executing request: select count(*) from Person.Person")
		os.Exit(0)
	}

	for rows.Next() {
		var count int
		err = rows.Scan(&count)
		fmt.Println("Results: ", count)
	}
	rows.Close()

	for i := 0; i < len(json_config.FieldToCompare); i++ {
		if json_config.FieldToCompare[i].Field1 != "" {
			var lastPoint = strings.LastIndex(json_config.FieldToCompare[i].Field1, ".")
			if lastPoint > 0 {
				var tablename = json_config.FieldToCompare[i].Field1[0:lastPoint]
				fmt.Println("tablename=" + tablename)
				var query = "select " + json_config.FieldToCompare[i].Field1 + ", count(*) from " + tablename + " group by " + json_config.FieldToCompare[i].Field1 + " order by " + json_config.FieldToCompare[i].Field1
				fmt.Println(query)
				rows, err := db1.Query(query)
				if err != nil {
					fmt.Println("Error while executing request: " + query)
					os.Exit(0)
				}
				defer rows.Close()
				fmt.Printf("md5 of result: %x \n", getMD5Result(rows))
			}
		}
	}
}
