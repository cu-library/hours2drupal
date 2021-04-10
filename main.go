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
	// HoursPath is the path to append to the target to build the full URL for Hours nodes.
	HoursPath = "/jsonapi/node/hours"
	// HoursByDayPath is the path to append to the target to build the full URL for hours_by_day paragraphs.
	HoursByDayPath = "/jsonapi/paragraph/hours_by_day"
	// RequestTimeout is the amount of time the tool will wait for API calls to complete before they are cancelled.
	RequestTimeout = 60 * time.Second
	// AcceptHeader is the MIME type Drupal's JSON API expects to see in the Accept header of POST requests.
	AcceptHeader = "application/vnd.api+json"
	// ContentTypeHeader is the MIME type Drupal's JSON API expects to see in the Content-Type header of POST requests.
	ContentTypeHeader = "application/vnd.api+json"
)

// ErrNoHeader is an error which is returned when a CSV file doesn't have a header line.
var ErrNoHeader = errors.New("csv file did not have a header")

// ErrMissingData is an error which is returned when a CSV file has missing fields.
var ErrMissingData = errors.New("missing data")

// ErrAPIError is an error which is returned when the Drupal API returns an unexpected error.
var ErrAPIError = errors.New("an API error occurred")

// HoursByDayParagraph is the struct compliment of the required JSON for an hours by day paragraph.
type HoursByDayParagraph struct {
	Data struct {
		Type       string `json:"type"`
		ID         string `json:"id,omitempty"`
		Attributes struct {
			DrupalInternalID         int    `json:"drupal_internal__id,omitempty"`
			DrupalInternalRevisionID int    `json:"drupal_internal__revision_id,omitempty"`
			ParentID                 string `json:"parent_id"`
			ParentType               string `json:"parent_type"`
			ParentFieldName          string `json:"parent_field_name"`
			BuildingHours            string `json:"field_building_hours"`
			ChatHours                string `json:"field_chat_hours"`
			Day                      string `json:"field_day"`
			Note                     string `json:"field_note"`
		} `json:"attributes"`
	} `json:"data"`
}

// NewHoursByDayParagraph creates a new NewHoursByDayParagraph struct.
func NewHoursByDayParagraph(parentID, buildingHours, chatHours, day, note string) HoursByDayParagraph {
	p := HoursByDayParagraph{}
	p.Data.Type = "paragraph--hours_by_day"
	p.Data.Attributes.ParentID = parentID
	p.Data.Attributes.ParentType = "node"
	p.Data.Attributes.ParentFieldName = "field_day"
	p.Data.Attributes.BuildingHours = strings.TrimSpace(buildingHours)
	p.Data.Attributes.ChatHours = strings.TrimSpace(chatHours)
	p.Data.Attributes.Day = strings.TrimSpace(day)
	p.Data.Attributes.Note = strings.TrimSpace(note)

	return p
}

// Post uses the JSON API endpoint at target to create the new paragraph.
func (p *HoursByDayParagraph) Post(ctx context.Context, target, username, password string) error {
	url := fmt.Sprintf("https://%v%v", target, HoursByDayPath)
	return p.doAPICall(ctx, url, http.MethodPost, username, password)
}

// doAPICall calls the API using the provided method.
func (p *HoursByDayParagraph) doAPICall(ctx context.Context, url, method, username, password string) error {
	// Create a new context from the base context with a timeout.
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	b, err := json.Marshal(p)
	if err != nil {
		return err
	}

	r, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(b))
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

	// If the response is 200 or 201, update the node.
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		rb, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		err = json.Unmarshal(rb, p)
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

// HoursNode is the struct compliment of the required JSON for an hours node.
type HoursNode struct {
	Data struct {
		Type       string `json:"type"`
		ID         string `json:"id,omitempty"`
		Attributes struct {
			Title string `json:"title"`
		} `json:"attributes"`
		Relationships struct {
			FieldDay struct {
				Data []ParagraphRelationship `json:"data"`
			} `json:"field_day"`
		} `json:"relationships,omitempty"`
	} `json:"data"`
}

// ParagraphRelationship contains the data linking the node to the paragraph.
type ParagraphRelationship struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	Meta struct {
		TargetRevisionID int `json:"target_revision_id"`
	} `json:"meta"`
}

// NewHoursNode creates a new HoursNode struct.
func NewHoursNode(title string) HoursNode {
	n := HoursNode{}
	n.Data.Type = "node--hours"
	n.Data.Attributes.Title = strings.TrimSpace(title)

	return n
}

// NewParagraphRelationship creates a new ParagraphRelationship.
func NewParagraphRelationship(pType, pID string, targetRevisionID int) ParagraphRelationship {
	p := ParagraphRelationship{}
	p.Type = pType
	p.ID = pID
	p.Meta.TargetRevisionID = targetRevisionID

	return p
}

// Post uses the JSON API endpoint at target to create the new node.
func (n *HoursNode) Post(ctx context.Context, target, username, password string) error {
	url := fmt.Sprintf("https://%v%v", target, HoursPath)
	return n.doAPICall(ctx, url, http.MethodPost, username, password)
}

// Patch uses the JSON API endpoint at target to update the new node.
func (n *HoursNode) Patch(ctx context.Context, target, username, password string) error {
	url := fmt.Sprintf("https://%v%v/%v", target, HoursPath, n.Data.ID)
	return n.doAPICall(ctx, url, http.MethodPatch, username, password)
}

// doAPICall calls the API using the provided method.
func (n *HoursNode) doAPICall(ctx context.Context, url, method, username, password string) error {
	// Create a new context from the base context with a timeout.
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	b, err := json.Marshal(n)
	if err != nil {
		return err
	}

	r, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(b))
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

	// If the response is 200 or 201, update the node.
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		rb, err := io.ReadAll(resp.Body)

		if err != nil {
			return err
		}

		err = json.Unmarshal(rb, n)
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

// DailyHours stores the data from the CSV file, the source data for the Drupal paragraphs.
type DailyHours struct {
	Day           time.Time
	Note          string
	BuildingHours string
	ChatHours     string
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

	fmt.Printf("Going to import hours into 'https://%v'.\n", *target)
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
	hours := []DailyHours{}

	// Load input from CSV files.
	for _, arg := range args {
		h, err := loadFromCSV(arg)
		if err != nil {
			return fmt.Errorf("processing CSV file '%v' failed, %w", arg, err)
		}

		hours = append(hours, h...)
	}

	// Partition the days by month.
	months := map[string][]DailyHours{}

	for _, h := range hours {
		monthAndYear := h.Day.Format("January, 2006")
		months[monthAndYear] = append(months[monthAndYear], h)
	}

	// Create a context which can be cancelled by a SIGINT signal.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// For every month, we create the 'container' node, then the containing paragraphs
	// which are then patched in.
	for month, dailyHours := range months {
		fmt.Printf("%v...", month)
		n := NewHoursNode(month)

		err := n.Post(ctx, target, username, password)
		if err != nil {
			return err
		}

		for _, h := range dailyHours {
			// Has our context been cancelled?
			if ctx.Err() != nil {
				return ctx.Err()
			}

			p := NewHoursByDayParagraph(n.Data.ID, h.BuildingHours, h.ChatHours, h.Day.Format("2006-01-02"), h.Note)

			err := p.Post(ctx, target, username, password)
			if err != nil {
				return err
			}

			r := NewParagraphRelationship(p.Data.Type, p.Data.ID, p.Data.Attributes.DrupalInternalRevisionID)
			n.Data.Relationships.FieldDay.Data = append(n.Data.Relationships.FieldDay.Data, r)

			err = n.Patch(ctx, target, username, password)
			if err != nil {
				return err
			}
		}

		fmt.Println(" Success")
	}

	return nil
}

// loadFromCSV processes one of the provided hours CSV files.
func loadFromCSV(arg string) (hours []DailyHours, err error) {
	f, err := os.Open(arg)
	if err != nil {
		return hours, err
	}

	r := csv.NewReader(f)

	// A map of column names to indexes.
	h := map[string]int{}

	// If the first line doesn't exist, return the header error.
	l, err := r.Read()
	if errors.Is(err, io.EOF) {
		return hours, ErrNoHeader
	}

	if err != nil {
		return hours, err
	}

	// Build the column name map from the header line.
	for i, header := range l {
		h[strings.TrimSpace(header)] = i
	}

	// Keep track of the line number for error reporting.
	lineNum := 1

	for {
		lineNum++

		l, err := r.Read()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return hours, err
		}

		// Pull the data from the line using the header map, trimming leading and trailing space.
		note := strings.TrimSpace(l[h["note"]])
		buildingHours := strings.TrimSpace(l[h["building hours"]])
		chatHours := strings.TrimSpace(l[h["chat hours"]])

		day := strings.TrimSpace(l[h["day"]])
		if day == "" {
			return hours, fmt.Errorf("%w: empty day on line %v", ErrMissingData, lineNum)
		}

		// Parse the day into a Time so we can more easily process it later.
		// The reference time is documented here: https://golang.org/pkg/time/#Parse
		parsedDay, err := time.Parse("2006-01-02", day)
		if err != nil {
			return hours, fmt.Errorf("Could not parse day on line %v: %w", lineNum, err)
		}

		if buildingHours == "" {
			return hours, fmt.Errorf("%w: empty building hours on line %v", ErrMissingData, lineNum)
		}

		if chatHours == "" {
			return hours, fmt.Errorf("%w: empty chat hours on line %v", ErrMissingData, lineNum)
		}

		n := DailyHours{
			Day:           parsedDay,
			Note:          note,
			BuildingHours: buildingHours,
			ChatHours:     chatHours,
		}

		hours = append(hours, n)
	}

	return hours, nil
}
