package main

import (
	"errors"
	//"fmt"
	"github.com/xwb1989/sqlparser"
)

// CSVGenerator struct
type CSVGenerator struct {
	extractColumns []string
	workers        int
	lineCount      int64
}

// NewGenerator constructor
func NewGenerator(cols []string, workers int) *CSVGenerator {
	return &CSVGenerator{
		extractColumns: cols,
		workers:        workers,
	}
}

// Generate generates CSV lines from sql INSERT statements
func (g *CSVGenerator) Generate(statements chan sqlparser.Statement) (csvLines chan []string, err error) {
	csvLines = make(chan []string)

	// Fetch columns first
	colIndexes, err := getColIndexes(statements, g.extractColumns)
	if err != nil {
		return nil, err
	}

	done := make(chan bool)

	for i := 0; i < g.workers; i++ {
		go parseInserts(statements, colIndexes, csvLines, done)
	}

	go func() {
		for i := 0; i < g.workers; i++ {
			<-done
		}

		close(csvLines)
	}()

	return
}

func getColIndexes(statements chan sqlparser.Statement, extractCols []string) (colIndexes []int, err error) {
	var parsedCols []string

L:
	for stmt := range statements {
		switch stmt := stmt.(type) {
		case *sqlparser.DDL:
			// Get columns from CREATE TABLE statement
			if stmt.Action != sqlparser.CreateStr {
				continue
			}

			for _, col := range stmt.TableSpec.Columns {
				parsedCols = append(parsedCols, col.Name.String())
			}

			break L
		}
	}

	if len(parsedCols) == 0 {
		err = errors.New("Did not encounter a CREATE TABLE statement")
		return
	}

	for _, col := range extractCols {
		if stringInSlice(col, parsedCols) == false {
			err = errors.New("Invalid columns in --columns flag")
			return
		}
	}

	for _, extractCol := range extractCols {
		for idx, parsedCol := range parsedCols {
			if extractCol == parsedCol {
				colIndexes = append(colIndexes, idx)
			}
		}
	}

	return
}

func parseInserts(statements chan sqlparser.Statement, colIndexes []int, csvLines chan []string, done chan bool) {
	for stmt := range statements {
		switch insert := stmt.(type) {
		case *sqlparser.Insert:

			// VALUES
			for _, values := range insert.Rows.(sqlparser.Values) {
				var line []string

				// Individual column values
				for _, colIndex := range colIndexes {
					for idx, val := range values {
						if idx == colIndex {
							// We are interested in this column
							switch v := val.(type) {
							case *sqlparser.SQLVal:
								line = append(line, string(v.Val))
							case *sqlparser.NullVal:
								line = append(line, "\\N")
							}
						}
					}
				}
				/*
					outputLine := make([]string, len(line))
					copy(outputLine, line)
				*/
				csvLines <- line

				//line = nil
			}
		}
	}

	done <- true
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
