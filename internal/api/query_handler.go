package api

import "net/http"

func (server *Server) health(writer http.ResponseWriter, request *http.Request) {
	report := server.runtime.Diagnostics()
	snapshot := server.runtime.Snapshot()
	status := http.StatusOK
	if !report.Healthy || !snapshot.Healthy() {
		status = http.StatusServiceUnavailable
	}
	writeJSON(writer, status, map[string]any{
		"status":     "available",
		"healthy":    report.Healthy,
		"checked_at": report.CheckedAt,
		"checks":     report.Checks,
		"failures":   report.Failures(),
	})
}

func (server *Server) operations(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"operations": server.runtime.Operations()})
}

func (server *Server) equipment(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"equipment": server.runtime.Equipment()})
}

func (server *Server) interlocks(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"interlocks": server.runtime.Interlocks()})
}

func (server *Server) incidents(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"incidents": server.runtime.Incidents()})
}

func (server *Server) snapshot(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, server.runtime.Snapshot())
}

func (server *Server) handovers(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"handovers": server.runtime.Handovers()})
}
