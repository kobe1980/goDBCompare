# goDBCompare



goDBCompare
===========

The aim of this project is to provide a simple solution to compare Postgresql DBs in case of a simple schema/database transformation. 

The configuration of the application is based on a json config file.
Here is an example based on AdventureWorks DB available here: https://github.com/lorint/AdventureWorks-for-Postgres

    {
        "Databases":[
                {
                        "Host": "localhost",
                        "Port": 5432,
                        "Login": "user1"
                        "Password": "user1",
                        "DatabaseName": "test1"
                },

                {
                        "Host": "localhost",
                        "Port": 5432,
                        "Login": "user2",
                        "Password": "user2",
                        "DatabaseName": "test2"
                }
        ],

        "FieldToCompare": [
                {"Field1": "Person.Person.firstname", "Field2": "Person.People.firstname"},
                {"Field1": "Person.Person.title", "Field2": "Person.People.title"},
                {"Field1": "Person.emailaddress.businessentityid", "Field2": "Person.emailaddress.businessentityid"},
                {"Field1": "Person.emailaddress.businessentityid", "Field2": "Person.emailaddress.businessentityid"}
        ]
	}

Dependencies
------------

 - Lib pq: https://github.com/lib/pq
 - go.uuid: https://github.com/satori/go.uuid
