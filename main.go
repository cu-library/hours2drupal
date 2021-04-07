// Copyright 2021 Carleton University Library.
// All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file.

// Command hours2drupal creates building hours in Drupal 9 from a CSV.
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
)

const (
	// ProjectName is the name of the executable, as displayed to the user in usage and version messages.
	ProjectName = "hours2drupal"
	// Version  is the version number, which should be overwritten when building using ldflags.
	Version = "devel"
)

// HoursNode is the struct compliment of the required JSON.
type HoursNode struct {
	Data struct {
		Type       string `json:"type"`
		Attributes struct {
			Title string `json:"title"`
			Date  string `json:"field_for_date"`
			Hours string `json:"field_hours_building_hours"`
			Note  string `json:"field_hours_note"`
		} `json:"attributes"`
	} `json:"data"`
}

// NewHoursNode creates a new HoursNode struct.
func NewHoursNode(title, date, hours, note string) HoursNode {
	n := HoursNode{}
	n.Data.Type = "node--hours"
	n.Data.Attributes.Title = strings.TrimSpace(title)
	n.Data.Attributes.Date = strings.TrimSpace(date)
	n.Data.Attributes.Hours = strings.TrimSpace(hours)
	n.Data.Attributes.Note = strings.TrimSpace(note)

	return n
}

// ErrNoHeader is an error which is returned when a CSV file doesn't have a header line.
var ErrNoHeader = errors.New("csv file did not have a header")

func main() {
	// Set the prefix of the default logger to the empty string.
	log.SetFlags(0)

	// Define the command line flags
	target := flag.String("target", "library.carleton.ca", "The name of the server to POST hours to.")
	printVersion := flag.Bool("version", false, "Print the version then exit.")
	printHelp := flag.Bool("help", false, "Print help documentation then exit.")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "%v\n", ProjectName)
		fmt.Fprintf(flag.CommandLine.Output(), "Process CSV files of hours, "+
			"and import them into a target Drupal 9 website. %v\n", Version)
		fmt.Fprintf(flag.CommandLine.Output(), "Version %v\n", Version)
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %v [FLAGS] file [file...]\n", Version)
		flag.PrintDefaults()
	}

	// Process the flags and arguments.
	flag.Parse()

	// Quick exit for help and version flags.
	if *printVersion {
		fmt.Printf("%v - Version %v.\n", ProjectName, Version)
		os.Exit(0)
	}

	if *printHelp {
		flag.CommandLine.SetOutput(os.Stdout)
		flag.Usage()
		os.Exit(0)
	}

	// Check that the slice of arguments (csv files to import) is not empty.
	if len(flag.Args()) == 0 {
		log.Println("Please provide at least one CSV file as an argument.")
		os.Exit(1)
	}

	fmt.Printf("Going to import hours into 'https://%v/jsonapi/hours'.\n", *target)

	err := process(flag.Args())
	if err != nil {
		log.Fatalf("Error: %v.\n", err)
	}
}

// process creates a context and processes the arguments.
func process(args []string) error {
	// Create a base context which can be cancelled by a SIGINT signal.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Process in input CSV files
	for _, arg := range args {
		// Has our context been cancelled?
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := processCSV(ctx, arg)
		if err != nil {
			return fmt.Errorf("processing CSV file '%v' failed, %w", arg, err)
		}
	}

	return nil
}

// processCSV processes one of the provided hours CSV files.
func processCSV(ctx context.Context, arg string) error {
	f, err := os.Open(arg)
	if err != nil {
		return err
	}

	r := csv.NewReader(f)

	// A map of column names to indexes.
	h := map[string]int{}

	// If the first line doesn't exist, return the header error.
	l, err := r.Read()
	if errors.Is(err, io.EOF) {
		return ErrNoHeader
	}

	if err != nil {
		return err
	}

	// Build the column name map from the header line.
	for i, header := range l {
		h[strings.TrimSpace(header)] = i
	}

	for {
		// Has our context been cancelled?
		if ctx.Err() != nil {
			return ctx.Err()
		}

		l, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return err
		}

		n := NewHoursNode(l[h["title"]], l[h["date"]], l[h["hours"]], l[h["note"]])
		b, err := json.Marshal(n)

		if err != nil {
			return err
		}

		fmt.Println(l)
		fmt.Println(string(b))
	}

	return nil
}
