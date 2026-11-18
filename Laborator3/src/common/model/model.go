// Package model holds the resources exchanged over HTTP (contracts/http-api.md)
// with their JSON and XML representations.
package model

import (
	"encoding/xml"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Employee is the resource kept by the data warehouse.
// Version, UpdatedAt and UpdatedBy are set by the warehouse, never by clients.
type Employee struct {
	XMLName    xml.Name  `json:"-" xml:"employee"`
	ID         string    `json:"id" xml:"id,attr"`
	Name       string    `json:"name" xml:"name"`
	Department string    `json:"department" xml:"department"`
	Position   string    `json:"position" xml:"position"`
	Email      string    `json:"email" xml:"email"`
	Salary     int64     `json:"salary" xml:"salary"`
	Version    int64     `json:"version" xml:"version"`
	UpdatedAt  time.Time `json:"updated_at" xml:"updated_at"`
	UpdatedBy  string    `json:"updated_by" xml:"updated_by"`
}

// EmployeePatch is the body of POST /employees/{id}: only the fields present
// are changed. Each field is set to an absolute value (never incremented), so
// applying the same patch twice gives the same state.
type EmployeePatch struct {
	XMLName    xml.Name `json:"-" xml:"employee"`
	Name       *string  `json:"name,omitempty" xml:"name,omitempty"`
	Department *string  `json:"department,omitempty" xml:"department,omitempty"`
	Position   *string  `json:"position,omitempty" xml:"position,omitempty"`
	Email      *string  `json:"email,omitempty" xml:"email,omitempty"`
	Salary     *int64   `json:"salary,omitempty" xml:"salary,omitempty"`
}

// EmployeeList is one page of GET /employees?offset=&limit=.
type EmployeeList struct {
	XMLName xml.Name   `json:"-" xml:"employees"`
	Offset  int        `json:"offset" xml:"offset,attr"`
	Limit   int        `json:"limit" xml:"limit,attr"`
	Total   int        `json:"total" xml:"total,attr"`
	Items   []Employee `json:"items" xml:"employee"`
}

// ChangeFeed is the answer of GET /updates/employees (pull by the source nodes).
type ChangeFeed struct {
	XMLName xml.Name   `json:"-" xml:"updates"`
	Cursor  string     `json:"cursor" xml:"cursor,attr"`
	Items   []Employee `json:"items" xml:"employee"`
}

// Error is the body of every 4xx/5xx answer.
type Error struct {
	XMLName       xml.Name `json:"-" xml:"error"`
	Code          string   `json:"code" xml:"code"`
	Message       string   `json:"message" xml:"message"`
	CorrelationID string   `json:"correlation_id,omitempty" xml:"correlation_id,omitempty"`
}

var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidID reports whether id can name an employee.
func ValidID(id string) bool { return idPattern.MatchString(id) }

// ErrInvalid marks a validation error (HTTP 400).
var ErrInvalid = errors.New("invalid")

// Validate checks the client-supplied fields.
func (e *Employee) Validate() error {
	e.Name = strings.TrimSpace(e.Name)
	e.Email = strings.TrimSpace(e.Email)
	switch {
	case !ValidID(e.ID):
		return fmt.Errorf("%w: id must match %s", ErrInvalid, idPattern)
	case e.Name == "" || len(e.Name) > 200:
		return fmt.Errorf("%w: name is required (max 200 characters)", ErrInvalid)
	case e.Salary < 0:
		return fmt.Errorf("%w: salary must be >= 0", ErrInvalid)
	case e.Email != "" && !strings.Contains(e.Email, "@"):
		return fmt.Errorf("%w: email is not valid", ErrInvalid)
	case len(e.Department) > 200 || len(e.Position) > 200 || len(e.Email) > 200:
		return fmt.Errorf("%w: fields are limited to 200 characters", ErrInvalid)
	}
	return nil
}

// SameContent reports whether a and b carry the same client data
// (the fields set by the warehouse are ignored).
func SameContent(a, b Employee) bool {
	return a.ID == b.ID && a.Name == b.Name && a.Department == b.Department &&
		a.Position == b.Position && a.Email == b.Email && a.Salary == b.Salary
}

// Empty reports whether the patch changes nothing.
func (p EmployeePatch) Empty() bool {
	return p.Name == nil && p.Department == nil && p.Position == nil && p.Email == nil && p.Salary == nil
}

// Apply returns e with the patch applied.
func (p EmployeePatch) Apply(e Employee) Employee {
	if p.Name != nil {
		e.Name = *p.Name
	}
	if p.Department != nil {
		e.Department = *p.Department
	}
	if p.Position != nil {
		e.Position = *p.Position
	}
	if p.Email != nil {
		e.Email = *p.Email
	}
	if p.Salary != nil {
		e.Salary = *p.Salary
	}
	return e
}
