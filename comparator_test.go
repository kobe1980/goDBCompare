// comparator_test.go
package main

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

var db *sql.DB

func getDB() *sql.DB {
	db, err := createDBConnection("postgres", "inge", "inge", "test1", "localhost", "5432")
	if err != nil {
		os.Exit(0)
	}
	return db
}

func TestCompareRows(t *testing.T) {
	var c = make(chan bool)
	go CompareRows("Person.Person.firstname", "Person.Person.firstname", db, db, c, "TEST")
	result := <-c
	fmt.Printf("Result = %t\n", result)
	if result != true {
		t.Error("Comparaison between same fields should be equal")
	}
	fmt.Println("Test Finished")
}

func TestMain(m *testing.M) {

	fmt.Println("Starting test suite")
	db = getDB()

	ret := m.Run()

	db.Close()
	fmt.Println("Test Suite completed")
	os.Exit(ret)
}
