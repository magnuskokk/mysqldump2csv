package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var extractColumns string

func init() {
	flag.StringVar(&extractColumns, "columns", "", "Columns to extract")
	flag.Parse()

	if extractColumns == "" {
		panic("--columns must be set")
	}

	fi, err := os.Stdin.Stat()
	if err != nil {
		panic(err)
	}

	if fi.Mode()&os.ModeNamedPipe == 0 {
		panic("Need input pipe")
	}
}

func main() {
	done := make(chan bool, 1)

	cols := strings.Split(extractColumns, ",")

	reader := bufio.NewReader(os.Stdin)

	parser := NewParser()
	statements := parser.Parse(reader)

	generator := NewGenerator(cols, 4)
	csvLines, err := generator.Generate(statements)
	if err != nil {
		parser.Quit()
		panic(err)
	}

	w := csv.NewWriter(os.Stdout)
	w.Comma = '\t'

	go csvOutput(w, csvLines, done)

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	<-done
}

func csvOutput(w *csv.Writer, csvLines chan []string, done chan bool) {
	for line := range csvLines {
		err := w.Write(line)
		if err != nil {
			panic(err)
		}
	}

	w.Flush()

	done <- true
}
