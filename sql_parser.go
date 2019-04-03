package main

import (
	"bufio"
	"bytes"
	//"fmt"
	"github.com/xwb1989/sqlparser"

	"io"
	//	"strings"
	"sync/atomic"
)

// Parser parses SQL rows to statements
type Parser interface {
	Parse(*io.Reader) chan sqlparser.Statement
	Quit()
	ByteCount() int64
}

// MySQLParser struct
type MySQLParser struct {
	byteCount int64
	quit      chan bool
}

// NewParser constructor
func NewParser() *MySQLParser {
	return &MySQLParser{
		quit: make(chan bool, 1),
	}
}

// Parse returns a channel of parsed sql statements and closes it when done
func (p *MySQLParser) Parse(r *bufio.Reader) chan sqlparser.Statement {
	statements := make(chan sqlparser.Statement, 1)

	go statementFetcher(r, statements, &p.byteCount, p.quit)

	return statements
}

// Quit the parser
func (p *MySQLParser) Quit() {
	p.quit <- true
}

// ByteCount returns the number of bytes read from input
func (p *MySQLParser) ByteCount() int64 {
	return atomic.LoadInt64(&p.byteCount)
}

func statementFetcher(r *bufio.Reader, output chan sqlparser.Statement, n *int64, quit chan bool) {
	var buffer bytes.Buffer

	var isEOF bool

	for {
		select {
		case <-quit:
			close(output)
			return
		default:
			if isEOF {
				close(output)
				return
			}
		}

		line, isPrefix, err := r.ReadLine()

		if err != nil {
			switch err {
			case io.EOF:
				isEOF = true
			default:
				panic(err)
			}
		}

		if _, err := buffer.Write(line); err != nil {
			panic(err)
		}

		if isPrefix == false {
			// Got the whole line

			switch {
			case bytes.HasPrefix(buffer.Bytes(), []byte("LOCK")):
				fallthrough
			case bytes.HasPrefix(buffer.Bytes(), []byte("UNLOCK")):
				fallthrough
			case bytes.HasPrefix(buffer.Bytes(), []byte("--")):
				fallthrough
			case bytes.HasPrefix(buffer.Bytes(), []byte("/*")):
				atomic.AddInt64(n, int64(buffer.Len()))

				buffer.Reset()
				continue

			case bytes.HasSuffix(buffer.Bytes(), []byte(";")):
				atomic.AddInt64(n, int64(buffer.Len()))

				// Got a whole statement
				output <- parseStatement(buffer.Bytes())
				buffer.Reset()
			}
		}
	}
}

func parseStatement(raw []byte) sqlparser.Statement {
	stmt, err := sqlparser.Parse(string(raw))
	if err != nil {
		panic(err)
	}

	return stmt
}
