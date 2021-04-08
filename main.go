// Copyright 2021 Carleton University Library.
// All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file.

// Command hours2drupal creates building hours in Drupal 9 from a CSV.
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"golang.org/x/term"
)

const (
	// ProjectName is the name of the executable, as displayed to the user in usage and version messages.
	ProjectName = "hours2drupal"
	// Version  is the version number, which should be overwritten when building using ldflags.
	Version = "devel"
	// HoursPostPath is the path to append to the target to build the full URL for POSTing JSON data.
	HoursPostPath = "/jsonapi/node/hours"
	// RequestTimeout is the amount of time the tool will wait for API calls to complete before they are cancelled.
	RequestTimeout = 60 * time.Second
	// AcceptHeader is the MIME type Drupal's JSON API expects to see in the Accept header of POST requests.
	AcceptHeader = "application/vnd.api+json"
	// ContentTypeHeader is the MIME type Drupal's JSON API expects to see in the Content-Type header of POST requests.
	ContentTypeHeader = "application/vnd.api+json"
)

// ErrNoHeader is an error which is returned when a CSV file doesn't have a header line.
var ErrNoHeader = errors.New("csv file did not have a header")

// ErrAPIError is an error which is returned when the Drupal API returns an unexpected error.
var ErrAPIError = errors.New("an API error occurred")

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

// Post uses the JSON API endpoint at target to create the new node.
func (n HoursNode) Post(ctx context.Context, target, username, password string) error {
	// Create a new context from the base context with a timeout.
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	// Build the URL.
	url := fmt.Sprintf("https://%v%v", target, HoursPostPath)

	b, err := json.Marshal(n)
	if err != nil {
		return err
	}

	r, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}

	// Set the required headers.
	r.Header.Set("Accept", AcceptHeader)
	r.Header.Set("Content-Type", ContentTypeHeader)
	r.SetBasicAuth(username, password)

	// Do the POST request.
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return err
	}

	// If the response is 201, return early.
	if resp.StatusCode == http.StatusCreated {
		// Drain and close the body.
		_, err = io.Copy(io.Discard, resp.Body)
		if err != nil {
			return err
		}

		err = resp.Body.Close()
		if err != nil {
			return err
		}

		return nil
	}

	// Some error occurred, return more details to the caller.
	body, err := io.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	err = resp.Body.Close()
	if err != nil {
		return err
	}

	return fmt.Errorf("%w: %v %v failed [%v]\n%v", ErrAPIError, r.Method, r.URL.String(), resp.StatusCode, string(body))
}

func main() {
	// Set the prefix of the default logger to the empty string.
	log.SetFlags(0)

	// Define the command line flags.
	target := flag.String("target", "library.carleton.ca", "The name of the server to POST hours to.")
	username := flag.String("username", "admin", "The username to use when authenticating with the target.")
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
		log.Fatalln("Please provide at least one CSV file as an argument.")
	}

	fmt.Printf("Going to import hours into 'https://%v%v'.\n", *target, HoursPostPath)
	fmt.Printf("Using username '%v'.\n", *username)

	// Read password for username.
	fmt.Printf("Password: ")

	pb, err := term.ReadPassword(int(os.Stdin.Fd()))

	fmt.Println()

	if err != nil {
		log.Fatalf("Error reading password: %v.\n", err)
	}

	password := string(pb)

	err = process(flag.Args(), *target, *username, password)
	if err != nil {
		log.Fatalf("Error: %v.\n", err)
	}
}

// process creates a context and processes the arguments.
func process(args []string, target, username, password string) error {
	// Create a base context which can be cancelled by a SIGINT signal.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Process input CSV files.
	for _, arg := range args {
		// Has our context been cancelled?
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := processCSV(ctx, arg, target, username, password)
		if err != nil {
			return fmt.Errorf("processing CSV file '%v' failed, %w", arg, err)
		}
	}

	return nil
}

// processCSV processes one of the provided hours CSV files.
func processCSV(ctx context.Context, arg, target, username, password string) error {
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

		fmt.Printf("%v... ", l[h["title"]])

		err = n.Post(ctx, target, username, password)
		if err != nil {
			fmt.Printf("Error\n")
			return err
		}

		fmt.Printf("Success\n")

		// Wait to not overwhelm server.
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
