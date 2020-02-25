package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	avro "github.com/linkedin/goavro/v2"
)

const (
	TYPE_STRING  = "string"
	TYPE_LONG    = "long"
	TYPE_BOOLEAN = "boolean"
	TYPE_DOUBLE  = "double"
)

type Schema struct {
	Name      string        `json:"name"`
	Type      string        `json:"type"`
	Namespace string        `json:"namespace"`
	Fields    []SchemaField `json:"fields"`
}

type SchemaField struct {
	Name string   `json:"name"`
	Type []string `json:"type"`
}

func init() {
	log.SetFlags(0)
}

func main() {
	if len(os.Args) < 2 {
		log.Printf("usage: %s <file>", os.Args[0])
		os.Exit(-1)
	}

	fileName := os.Args[1]
	log.Printf("reading %s", fileName)

	inHandle, err := os.Open(fileName)
	if err != nil {
		log.Printf("error: unable to open %s: %v", fileName, err)
		os.Exit(-1)
	}

	reader, err := avro.NewOCFReader(inHandle)
	if err != nil {
		log.Printf("error: unable to read avro file: %v", err)
		os.Exit(-1)
	}

	log.Printf("loading schema")
	rawSchema := reader.Codec().Schema()
	schema := &Schema{}
	json.Unmarshal([]byte(rawSchema), schema)

	log.Printf("resource name: %s", schema.Name)
	columns := []string{}
	for _, field := range schema.Fields {
		typ := strings.Join(field.Type, ", ")
		log.Printf("  - %s (%s)", field.Name, typ)
		columns = append(columns, field.Name)
	}

	outHandle := os.Stdout
	out := csv.NewWriter(outHandle)

	out.Write(columns)
	out.Flush()

	for reader.Scan() {
		row, err := reader.Read()
		if err != nil {
			log.Printf("error: unable to read row")
			os.Exit(-1)
		}

		values, err := readRow(row, schema)
		if err != nil {
			log.Printf("error: unable to read row: %v", err)
			os.Exit(-1)
		}

		out.Write(values)
		out.Flush()
	}
}

func readRow(row interface{}, schema *Schema) ([]string, error) {
	mapped, ok := row.(map[string]interface{})
	if !ok {
		return nil, errors.New("error: unable to decode row")
	}

	values := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		typ := strings.Replace(strings.Join(field.Type, ""), "null", "", 1)
		rawVal := mapped[field.Name]
		if rawVal == nil {
			continue
		}

		val := rawVal.(map[string]interface{})
		switch typ {
		case TYPE_STRING:
			values[i] = val["string"].(string)

		case TYPE_BOOLEAN:
			if val["boolean"].(bool) {
				values[i] = "TRUE"
			} else {
				values[i] = "FALSE"
			}

		case TYPE_LONG:
			numeric := val["long"].(int64)
			values[i] = strconv.FormatInt(numeric, 10)

		case TYPE_DOUBLE:
			numeric := val["double"].(float64)
			values[i] = strconv.FormatFloat(numeric, 'E', -1, 64)

		default:
			return nil, fmt.Errorf("unknown type: %s", typ)
		}
	}

	return values, nil
}
