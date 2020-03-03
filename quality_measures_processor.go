package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const (
	API_URL = "https://2swdepm0wa.execute-api.us-east-1.amazonaws.com/prod/NavaInterview/measures"

	CSV_NAME_INDEX      = 0
	CSV_WIDTH_INDEX     = 1
	CSV_DATA_TYPE_INDEX = 2

	DATA_TYPE_TEXT    = "TEXT"
	DATA_TYPE_INTEGER = "INTEGER"
	DATA_TYPE_BOOLEAN = "BOOLEAN"
)

type Field struct {
	Name  string
	Width int
	Type  string
}

func main() {
	// Get the list of schema files
	schemaFileNames, err := getSchemaFilenames()
	if err != nil {
		log.Panic(err)
	}

	for _, fileName := range schemaFileNames {
		// Get the schema information from the file
		schema, schemaErr := getSchema(fileName)
		if schemaErr != nil {
			log.Printf("Skipping schema file %s", fileName)
			continue
		}

		// Construct the data file name from the schema file name and load the records
		dotIndex := strings.LastIndex(fileName, ".")
		if dotIndex == -1 {
			log.Printf("Found schema filename with no '.': %s. Skipping...", fileName)
			continue
		}
		dataFileName := fileName[0:dotIndex] + ".txt"
		dataRecords, dataErr := getData(dataFileName)
		if dataErr != nil {
			log.Printf("Skipping data file %s", fileName)
			continue
		}

		// Iterate over the records from the file, parse & convert them to JSON, and post to the API
		for _, dataRecord := range dataRecords {
			recordJson, jsonErr := getRecordJson(dataRecord, schema)
			if jsonErr != nil {
				log.Printf("Skipping data record '%s'", dataRecord)
				continue
			}

			postRecord(recordJson)
		}
	}
}

// getSchemaFilenames returns a list of names of files in the schemas directory
func getSchemaFilenames() ([]string, error) {
	files, err := ioutil.ReadDir("schemas")
	if err != nil {
		log.Printf("Got error attempting to list schema files: %+v", err)
		return nil, err
	}

	fileNames := make([]string, len(files))
	for i, file := range files {
		fileNames[i] = file.Name()
	}

	return fileNames, nil
}

// getSchema is a helper function for parsing a CSV schema file and returning the records as a list.
func getSchema(fileName string) ([]Field, error) {
	csvFile, openErr := os.Open("schemas/" + fileName)
	if openErr != nil {
		log.Printf("Got error opening CSV file %s: %+v", fileName, openErr)
		return nil, openErr
	}

	// Close the file when we're done
	defer csvFile.Close()

	csvReader := csv.NewReader(bufio.NewReader(csvFile))
	records, parseErr := csvReader.ReadAll()
	if parseErr != nil {
		log.Printf("Got error parsing CSV file %s: %+v", fileName, parseErr)
		return nil, parseErr
	}

	fields := make([]Field, len(records))
	for i, record := range records {
		// Parse width as int
		width, err := strconv.Atoi(strings.TrimSpace(record[CSV_WIDTH_INDEX]))
		if err != nil {
			log.Printf("Found invalid integer value for width in CSV file %s, record %d: '%s'", fileName, i+1, record[1])
			return nil, err
		}

		// Validate data type
		dataType := record[CSV_DATA_TYPE_INDEX]
		if (dataType != DATA_TYPE_BOOLEAN) && (dataType != DATA_TYPE_INTEGER) && (dataType != DATA_TYPE_TEXT) {
			errMsg := fmt.Sprintf("Found invalid value for data type in CSV file %s, record %d: '%s'", fileName, i+1, dataType)
			log.Printf(errMsg)
			return nil, errors.New(errMsg)
		}

		field := Field{
			Name:  record[CSV_NAME_INDEX],
			Width: width,
			Type:  dataType,
		}
		fields[i] = field
	}

	return fields, nil
}

// getData reads the specified data file and returns the records as a list of strings
func getData(fileName string) ([]string, error) {
	dataFile, openErr := os.Open("data/" + fileName)
	if openErr != nil {
		log.Printf("Got error opening data file %s: %+v", fileName, openErr)
		return nil, openErr
	}

	// Close the file when we're done
	defer dataFile.Close()

	// Read the file line-by-line
	records := make([]string, 0, 10)
	fileReader := bufio.NewScanner(dataFile)
	for fileReader.Scan() {
		records = append(records, fileReader.Text())
	}
	if readErr := fileReader.Err(); readErr != nil {
		log.Printf("Got error reading data file %s: %+v", fileName, readErr)
		return records, readErr
	}

	return records, nil
}

// getRecordJson parses a record from the data file using the specified schema and converts it to a JSON-formatted string
func getRecordJson(dataRecord string, schema []Field) ([]byte, error) {
	data := make(map[string]interface{})

	startIndex := 0
	for _, field := range schema {
		fieldValue := strings.TrimSpace(dataRecord[startIndex:(startIndex + field.Width)])
		switch field.Type {
		case DATA_TYPE_BOOLEAN:
			boolVal, boolErr := strconv.ParseBool(fieldValue)
			if boolErr != nil {
				log.Printf("Found invalid boolean value: %s", fieldValue)
				return nil, boolErr
			}
			data[field.Name] = boolVal
		case DATA_TYPE_INTEGER:
			intVal, intErr := strconv.Atoi(fieldValue)
			if intErr != nil {
				log.Printf("Found invalid integer value: %s", fieldValue)
				return nil, intErr
			}
			data[field.Name] = intVal
		case DATA_TYPE_TEXT:
			data[field.Name] = fieldValue
		}

		// Increment the index to the start of the next field in the record
		startIndex += field.Width
	}

	jsonData, jsonErr := json.Marshal(data)
	if jsonErr != nil {
		log.Printf("Got error attempting to marshal record data into JSON: %+v", jsonErr)
	}
	return jsonData, jsonErr
}

// postRecord sends the input JSON data to the API in a POST request
func postRecord(jsonRecord []byte) {
	log.Printf("POSTing JSON data to API: %s", string(jsonRecord))
	resp, postErr := http.Post(API_URL, "application/json", bytes.NewBuffer(jsonRecord))

	if postErr != nil {
		log.Printf("Received error response from API: %+v", postErr)
	} else {
		defer resp.Body.Close()
		body, respErr := ioutil.ReadAll(resp.Body)
		if respErr != nil {
			log.Printf("Got error parsing API response: %+v", respErr)
		} else {
			log.Printf("Received %d response from API: %s", resp.StatusCode, string(body))
		}
	}
}
