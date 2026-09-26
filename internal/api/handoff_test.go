package api

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/wyw14/solo-035/internal/service"
)

func control(t *testing.T, server *Server, payload map[string]any) map[string]any {
	t.Helper()
	body, _ := json.Marshal(payload)
	request := httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	server.Router().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("control %v -> %d: %s", payload["action"], recorder.Code, recorder.Body.String())
	}
	var result struct {
		Value map[string]any `json:"value"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result.Value
}

func TestHandoverControlFlow(t *testing.T) {
	runtime, err := service.NewRuntime(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Bootstrap(runtime); err != nil {
		t.Fatal(err)
	}
	var web fs.FS = fstest.MapFS{"operations.html": &fstest.MapFile{Data: []byte("ok")}}
	server := NewServer(runtime, web)

	operations := httptest.NewRequest(http.MethodGet, "/api/operations", nil)
	opRecorder := httptest.NewRecorder()
	server.Router().ServeHTTP(opRecorder, operations)
	var ops struct {
		Operations []struct {
			EventID string `json:"event_id"`
			Name    string `json:"name"`
		} `json:"operations"`
	}
	json.Unmarshal(opRecorder.Body.Bytes(), &ops)
	var outgoingID string
	for _, op := range ops.Operations {
		if op.Name == "Evening News" {
			outgoingID = op.EventID
		}
	}
	if outgoingID == "" {
		t.Fatal("bootstrap operation not found")
	}

	createBody := bytes.NewReader([]byte(`{"name":"Late Show"}`))
	createRequest := httptest.NewRequest(http.MethodPost, "/api/operations", createBody)
	createRecorder := httptest.NewRecorder()
	server.Router().ServeHTTP(createRecorder, createRequest)
	if createRecorder.Code != http.StatusCreated {
		t.Fatalf("create -> %d: %s", createRecorder.Code, createRecorder.Body.String())
	}
	var incoming struct {
		ID string `json:"id"`
	}
	json.Unmarshal(createRecorder.Body.Bytes(), &incoming)

	control(t, server, map[string]any{"action": "rehearse-incoming", "event_id": incoming.ID})
	prepared := control(t, server, map[string]any{
		"action":            "prepare-handover",
		"event_id":          outgoingID,
		"incoming_event_id": incoming.ID,
	})
	token, _ := prepared["token"].(string)
	if token == "" {
		t.Fatal("prepare-handover returned no token")
	}

	cut := control(t, server, map[string]any{"action": "execute-handover", "handover_token": token})
	if cut["status"] != "on-air" {
		t.Fatalf("cut status = %v", cut["status"])
	}

	handoffRequest := httptest.NewRequest(http.MethodGet, "/api/handovers", nil)
	handoffRecorder := httptest.NewRecorder()
	server.Router().ServeHTTP(handoffRecorder, handoffRequest)
	if handoffRecorder.Code != http.StatusOK {
		t.Fatalf("handovers -> %d", handoffRecorder.Code)
	}
	var listed struct {
		Handovers []struct {
			Status string `json:"status"`
		} `json:"handovers"`
	}
	json.Unmarshal(handoffRecorder.Body.Bytes(), &listed)
	if len(listed.Handovers) != 1 || listed.Handovers[0].Status != "on-air" {
		t.Fatalf("handovers = %s", handoffRecorder.Body.String())
	}

	// The completed handover survives a checkpoint round trip.
	checkpoint := runtime.ExportCheckpoint()
	if len(checkpoint.Handoffs) != 1 {
		t.Fatalf("checkpoint handoffs = %d", len(checkpoint.Handoffs))
	}
	fresh, err := service.NewRuntime(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.RestoreCheckpoint(checkpoint); err != nil {
		t.Fatal(err)
	}
	if len(fresh.Handovers()) != 1 || fresh.Handovers()[0].Status != "on-air" {
		t.Fatalf("restored handoffs = %+v", fresh.Handovers())
	}
}
