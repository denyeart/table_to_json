/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric/core/chaincode/shim"
)

// SimpleChaincode example simple Chaincode implementation
type SimpleChaincode struct {
}

type Marble struct {
	ObjectType string `json:"docType"` //docType is used to distinguish the various types of objects in state database
	Name       string `json:"name"`    //the fieldtags are needed to keep case from bouncing around
	Color      string `json:"color"`
	Size       int    `json:"size"`
	Owner      string `json:"owner"`
}

// ============================================================================================================================
// Main
// ============================================================================================================================
func main() {
	err := shim.Start(new(SimpleChaincode))
	if err != nil {
		fmt.Printf("Error starting Simple chaincode: %s", err)
	}
}

// ============================================================================================================================
// Init - reset all the things
// ============================================================================================================================
func (t *SimpleChaincode) Init(stub shim.ChaincodeStubInterface) ([]byte, error) {

	// Create marble table with Color and Name as compound key
	stub.CreateTable("Marbles", []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "Color", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "Name", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "Size", Type: shim.ColumnDefinition_INT32, Key: false},
		&shim.ColumnDefinition{Name: "Owner", Type: shim.ColumnDefinition_STRING, Key: false},
	})

	fmt.Printf("Created the Marbles table")

	return nil, nil
}

// ============================================================================================================================
// Invoke - Our entry point for Invocations
// ============================================================================================================================
func (t *SimpleChaincode) Invoke(stub shim.ChaincodeStubInterface) ([]byte, error) {
	function, args := stub.GetFunctionAndParameters()
	fmt.Println("invoke is running " + function)

	// Handle different functions
	if function == "init" { //initialize the chaincode state, used as reset
		return t.Init(stub)
	} else if function == "init_marble_table" { //create a new marble
		return t.init_marble_table(stub, args)
	} else if function == "init_marble_json" { //create a new marble
		return t.init_marble_json(stub, args)
	} else if function == "get_marble_table" { //get a marble
		return t.get_marble_table(stub, args)
	} else if function == "get_marble_json" { //get a marble
		return t.get_marble_json(stub, args)
	} else if function == "get_blue_marbles_table" { //get blue marbles
		return t.get_blue_marbles_table(stub, args)
	} else if function == "get_blue_marbles_json" { //get blue marbles
		return t.get_blue_marbles_json(stub, args)
	} else if function == "set_owner" { //change owner of a marble
		res, err := t.set_owner(stub, args)
		return res, err
	}
	fmt.Println("invoke did not find func: " + function) //error

	return nil, errors.New("Received unknown function invocation")
}

// ============================================================================================================================
// Query - Our entry point for Queries
// ============================================================================================================================
func (t *SimpleChaincode) Query(stub shim.ChaincodeStubInterface) ([]byte, error) {
	function, args := stub.GetFunctionAndParameters()
	fmt.Println("query is running " + function)

	// Handle different functions
	if function == "read" { //read a variable
		return t.read(stub, args)
	}
	fmt.Println("query did not find func: " + function) //error

	return nil, errors.New("Received unknown function query")
}

// ============================================================================================================================
// Read - read a variable from chaincode state
// ============================================================================================================================
func (t *SimpleChaincode) read(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	var name, jsonResp string
	var err error

	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting name of the var to query")
	}

	name = args[0]
	valAsbytes, err := stub.GetState(name) //get the var from chaincode state
	if err != nil {
		jsonResp = "{\"Error\":\"Failed to get state for " + name + "\"}"
		return nil, errors.New(jsonResp)
	}

	return valAsbytes, nil //send it onward
}

// ============================================================================================================================
// Init Marble - create a new marble, store into chaincode state as a table record
// ============================================================================================================================
func (t *SimpleChaincode) init_marble_table(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	//   0       1       2     3
	// "name", "blue", "35", "bob"
	if len(args) != 4 {
		return nil, errors.New("Incorrect number of arguments. Expecting 4")
	}

	name := args[0]
	color := strings.ToLower(args[1])
	owner := strings.ToLower(args[3])
	size, _ := strconv.Atoi(args[2])
	objectType := "Marble"

	// Create marble from inputs
	marble := &Marble{objectType, name, color, size, owner}

	//Convert marble to a row matching the marble table definition
	row := shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: marble.Color}},
			&shim.Column{Value: &shim.Column_String_{String_: marble.Name}},
			&shim.Column{Value: &shim.Column_Int32{Int32: int32(marble.Size)}},
			&shim.Column{Value: &shim.Column_String_{String_: marble.Owner}}},
	}
	// Add marble row to state table
	_, err := stub.InsertRow("Marbles", row)
	if err != nil {
		return nil, err
	}

	fmt.Println("- end init marble")
	return nil, nil
}

// ============================================================================================================================
// Init Marble - create a new marble, store into chaincode state as a JSON record
// ============================================================================================================================
func (t *SimpleChaincode) init_marble_json(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	//   0       1       2     3
	// "name", "blue", "35", "bob"
	if len(args) != 4 {
		return nil, errors.New("Incorrect number of arguments. Expecting 4")
	}

	objectType := "Marble"
	name := args[0]
	color := strings.ToLower(args[1])
	size, _ := strconv.Atoi(args[2])
	owner := strings.ToLower(args[3])

	// Create marble from inputs
	marble := &Marble{objectType, name, color, size, owner}

	// Convert marble to JSON with Color and Name as compound key
	compoundKey, _ := t.createCompoundKey(objectType, []string{marble.Color, marble.Name})
	marbleJSONBytes, _ := json.Marshal(marble)

	// Add marble JSON to state
	stub.PutState(compoundKey, marbleJSONBytes)

	fmt.Println("- end init marble")
	return nil, nil
}

// ============================================================================================================================
// get marble from table
// ============================================================================================================================
func (t *SimpleChaincode) get_marble_table(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	tableName := "Marbles"

	name := args[0]
	color := args[1]
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: color}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: name}}
	columns = append(columns, col1)
	columns = append(columns, col2)

	row, _ := stub.GetRow(tableName, columns)

	marble := Marble{
		"Marble",
		row.Columns[0].GetString_(),
		row.Columns[1].GetString_(),
		int(row.Columns[2].GetInt32()),
		row.Columns[3].GetString_()}

	fmt.Println("got the marble from table: " + marble.Name)

	return nil, nil
}

// ============================================================================================================================
// get marble json
// ============================================================================================================================
func (t *SimpleChaincode) get_marble_json(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	// Define partial key to query within Marbles namespace (objectType)
	objectType := "Marble"
	name := args[0]
	color := args[1]
	compoundKey, _ := t.createCompoundKey(objectType, []string{color, name})

	marbleJSONBytes, _ := stub.GetState(compoundKey)

	marble := Marble{}
	json.Unmarshal(marbleJSONBytes, &marble)

	fmt.Println("got the JSON marble: " + marble.Name)

	return nil, nil
}

// ============================================================================================================================
// get blue marbles from table
// ============================================================================================================================
func (t *SimpleChaincode) get_blue_marbles_table(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	// Define partial key to query within Marbles namespace (tableName)
	tableName := "Marbles"
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: "blue"}}
	columns = append(columns, col1)

	// Query table using partial keys
	rowChannel, _ := stub.GetRows(tableName, columns)

	// Get marble rows from result set
	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)
				fmt.Printf("row=%v\n", row)
			}
		}
		if rowChannel == nil {
			break
		}
	}

	// Process returned marble records
	for _, row := range rows {
		fmt.Println("blue marble: " + row.Columns[1].GetString_()) //column 1 is marble name
	}

	return nil, nil
}

// ============================================================================================================================
// get blue marbles from json
// ============================================================================================================================
func (t *SimpleChaincode) get_blue_marbles_json(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	// Define partial key to query within Marbles namespace (objectType)
	objectType := "Marble"
	partialKeysForQuery := []string{"blue"} // First N of the compound keys can be chosen

	// Query state using partial keys
	keysIter, _ := t.partialCompoundKeyQuery(stub, objectType, partialKeysForQuery)
	defer keysIter.Close()

	// Get records from result set
	var marbles []Marble
	for keysIter.HasNext() {
		_, marbleJSONBytes, _ := keysIter.Next()
		marble := Marble{}
		json.Unmarshal(marbleJSONBytes, &marble)
		marbles = append(marbles, marble)
	}

	// Process returned marble records
	for _, marble := range marbles {
		fmt.Println("blue marble: " + marble.Name)
	}

	return nil, nil
}

// ============================================================================================================================
// Utility functions (may become chaincode APIs)
// ============================================================================================================================

func (t *SimpleChaincode) createCompoundKey(objectType string, keys []string) (string, error) {
	var keyBuffer bytes.Buffer
	keyBuffer.WriteString(objectType)
	for _, key := range keys {
		keyBuffer.WriteString(strconv.Itoa(len(key)))
		keyBuffer.WriteString(key)
	}
	return keyBuffer.String(), nil
}

func (t *SimpleChaincode) partialCompoundKeyQuery(stub shim.ChaincodeStubInterface, objectType string, keys []string) (shim.StateRangeQueryIteratorInterface, error) {
	// TODO - call RangeQueryState() based on the partial keys and pass back the iterator

	keyString, _ := t.createCompoundKey(objectType, keys)
	keysIter, err := stub.RangeQueryState(keyString+"1", keyString+":")
	if err != nil {
		return nil, fmt.Errorf("Error fetching rows: %s", err)
	}
	defer keysIter.Close()

	return keysIter, err
}

// ============================================================================================================================
// Set Owner Permission on Marble
// ============================================================================================================================
func (t *SimpleChaincode) set_owner(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	var err error

	//   0       1
	// "name", "bob"
	if len(args) < 2 {
		return nil, errors.New("Incorrect number of arguments. Expecting 2")
	}

	fmt.Println("- start set owner")
	fmt.Println(args[0] + " - " + args[1])
	marbleAsBytes, err := stub.GetState(args[0])
	if err != nil {
		return nil, errors.New("Failed to get thing")
	}
	res := Marble{}
	json.Unmarshal(marbleAsBytes, &res) //un stringify it aka JSON.parse()
	res.Owner = args[1]                 //change the owner

	jsonAsBytes, _ := json.Marshal(res)
	err = stub.PutState(args[0], jsonAsBytes) //rewrite the marble with id as key
	if err != nil {
		return nil, err
	}

	fmt.Println("- end set owner")
	return nil, nil
}
