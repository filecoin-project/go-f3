package observer

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

type QueryRequest struct {
	Query string `json:"Query"`
}

func (o *Observer) serveMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/query", o.queryHandler)
	return mux
}

func (o *Observer) queryHandler(w http.ResponseWriter, r *http.Request) {
	const (
		contentTypeJson = "application/json"
		contentTypeText = "text/plain"
	)
	switch r.Method {
	case http.MethodOptions:
		allowedMethods := []string{http.MethodOptions, http.MethodPost}
		acceptedContentTypes := []string{contentTypeJson, contentTypeText}
		r.Header.Set("Accept", strings.Join(acceptedContentTypes, ","))
		r.Header.Set("Allow", strings.Join(allowedMethods, ","))
		return
	case http.MethodPost:
		// Proceed to parse the request.
	default:
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	// Infer database query depending on request content type, accepting:
	//  * application/json: that corresponds to QueryRequest.
	//  * text/plain: where the request body is the database query.
	//  * unspecified: which is interpreted as text/plain.
	var requestMediaType, dbQuery string
	if requestContentType := r.Header.Get("Content-Type"); requestContentType != "" {
		var err error
		requestMediaType, _, err = mime.ParseMediaType(requestContentType)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid content type: %s", err), http.StatusBadRequest)
			return
		}
	}
	switch requestMediaType {
	case contentTypeJson:
		var req QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("Invalid request: %s", err), http.StatusBadRequest)
			return
		}
		dbQuery = req.Query
	case contentTypeText, "": // Interpret unspecified content type as text/plain
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Cannot read request: %s", err), http.StatusBadRequest)
			return
		}
		dbQuery = string(body)
	default:
		http.Error(w, fmt.Sprintf("%s: %s", http.StatusText(http.StatusUnsupportedMediaType), requestMediaType), http.StatusUnsupportedMediaType)
		return
	}

	rows, err := o.db.QueryContext(r.Context(), dbQuery)
	if err != nil {
		logger.Errorw("Failed to execute query", "err", err)
		http.Error(w, fmt.Sprintf("Failed to execute query: %s", err), http.StatusInternalServerError)
		return
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		logger.Errorw("Failed to get columns", "err", err)
		http.Error(w, fmt.Sprintf("Failed to get columns: %s", err), http.StatusInternalServerError)
		return
	}

	var results []map[string]any
	for rows.Next() {
		row := make([]any, len(columns))
		rowPointers := make([]any, len(columns))
		for i := range row {
			rowPointers[i] = &row[i]
		}
		if err := rows.Scan(rowPointers...); err != nil {
			http.Error(w, fmt.Sprintf("Failed to scan row: %s", err), http.StatusInternalServerError)
			return
		}
		rowByColumn := make(map[string]any)
		for i, name := range columns {
			rowByColumn[name] = row[i]
		}
		results = append(results, rowByColumn)
	}

	w.Header().Set("Content-Type", contentTypeJson)
	if err := json.NewEncoder(w).Encode(results); err != nil {
		logger.Errorw("Failed to write response", "err", err)
	}
}
