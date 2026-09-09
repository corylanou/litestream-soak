package flyapi

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

type DiagnosticMachine struct {
	ID         string                  `json:"id"`
	Name       string                  `json:"name"`
	State      string                  `json:"state"`
	Region     string                  `json:"region"`
	InstanceID string                  `json:"instance_id"`
	Config     DiagnosticMachineConfig `json:"config"`
	ImageRef   ImageRef                `json:"image_ref"`
	CreatedAt  time.Time               `json:"created_at"`
	UpdatedAt  time.Time               `json:"updated_at"`
	Events     []MachineEvent          `json:"events"`
}

type DiagnosticMachineConfig struct {
	Image  string  `json:"image"`
	Guest  Guest   `json:"guest,omitempty"`
	Mounts []Mount `json:"mounts,omitempty"`
}

func (m *Machine) Diagnostic() *DiagnosticMachine {
	if m == nil {
		return nil
	}
	return &DiagnosticMachine{
		ID: m.ID, Name: m.Name, State: m.State, Region: m.Region, InstanceID: m.InstanceID,
		Config:   DiagnosticMachineConfig{Image: m.Config.Image, Guest: m.Config.Guest, Mounts: slices.Clone(m.Config.Mounts)},
		ImageRef: m.ImageRef, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt, Events: slices.Clone(m.Events),
	}
}

func DiagnosticError(err error) string {
	if err == nil {
		return ""
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return fmt.Sprintf("Machine metadata unavailable (provider HTTP %d)", apiErr.StatusCode)
	}
	return "Machine metadata unavailable"
}
