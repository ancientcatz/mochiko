// Package mochiko provides a wrapper for loading multiple "anko-rui",
// with support for separate logging handlers and hot reloading. It builds internal
// indices for fast lookups, supports fuzzy searching, incremental reloading, and debounced events.
package mochiko

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"slices"

	"github.com/ancientcatz/anko"
	"github.com/fsnotify/fsnotify"
)

// Anko wraps an anko.Engine instance along with its source file path.
type Anko struct {
	Engine   *anko.Engine
	FilePath string
}

// Metadata returns the parsed metadata of the anko.
func (ext *Anko) Metadata() anko.Metadata {
	return ext.Engine.GetMetadata()
}

// Manager loads and indexes multiple anko anko-rui. It supports separate loggers
// and hot reloading with incremental updates and event debouncing.
type Manager struct {
	mu               sync.RWMutex
	ankoRui          []*Anko
	mochikoLogger    *slog.Logger
	ankoLogger       *slog.Logger
	dir              string
	watcher          *fsnotify.Watcher
	hotReloadStop    chan struct{}
	hotReloadAlive   bool
	debounceDuration time.Duration
	pendingEvents    map[string]fsnotify.Op

	// Index caches: keys are stored in lower-case.
	fileIndex         map[string]*Anko   // file path -> anko
	indexByIdentifier map[string][]*Anko // identifier -> anko-rui
	indexBySource     map[string][]*Anko // source -> anko-rui
	indexByName       map[string][]*Anko // name -> anko-rui (exact match)
}

// NewManager creates a new Manager with the provided mochiko logger and anko logger.
// If nil is passed for either logger, default loggers will be used.
func NewManager(mochikoLog, ankoLog *slog.Logger) *Manager {
	if mochikoLog == nil {
		mochikoLog = slog.Default()
	}
	if ankoLog == nil {
		ankoLog = slog.Default()
	}
	return &Manager{
		ankoRui:           []*Anko{},
		mochikoLogger:     mochikoLog,
		ankoLogger:        ankoLog,
		hotReloadStop:     make(chan struct{}),
		debounceDuration:  500 * time.Millisecond,
		pendingEvents:     make(map[string]fsnotify.Op),
		fileIndex:         make(map[string]*Anko),
		indexByIdentifier: make(map[string][]*Anko),
		indexBySource:     make(map[string][]*Anko),
		indexByName:       make(map[string][]*Anko),
	}
}

// SetMochikoLogger updates the logger used by the Manager.
func (m *Manager) SetMochikoLogger(logger *slog.Logger) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mochikoLogger = logger
}

// SetAnkoLogger updates the logger that is passed to each anko.Engine.
func (m *Manager) SetAnkoLogger(logger *slog.Logger) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ankoLogger = logger
}

// LoadDirectory scans the given directory for YAML files (with .yml or .yaml suffix),
// loads each anko via the anko engine (using the current anko logger) and rebuilds
// the indices. This is a full load which resets all anko-rui.
func (m *Manager) LoadDirectory(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.dir = dir
	m.ankoRui = []*Anko{}
	m.fileIndex = make(map[string]*Anko)

	entries, err := os.ReadDir(dir)
	if err != nil {
		m.mochikoLogger.Error("failed to read directory", "dir", dir, "error", err)
		return fmt.Errorf("failed to read directory %s: %w", dir, err)
	}
	var filePaths []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue // skip subdirectories
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".yml") && !strings.HasSuffix(name, ".yaml") {
			continue // skip non-YAML files
		}
		fullPath := filepath.Join(dir, name)
		engine := anko.NewEngine(m.ankoLogger)
		if err := engine.LoadFile(fullPath); err != nil {
			m.mochikoLogger.Error("failed to load anko", "file", fullPath, "error", err)
			return fmt.Errorf("failed to load anko from %s: %w", fullPath, err)
		}
		ext := &Anko{
			Engine:   engine,
			FilePath: fullPath,
		}
		m.ankoRui = append(m.ankoRui, ext)
		m.fileIndex[fullPath] = ext
		filePaths = append(filePaths, fullPath)
	}
	m.mochikoLogger.Debug("anko-rui loaded", "files", filePaths)
	m.rebuildIndicesLocked()
	return nil
}

// rebuildIndicesLocked rebuilds all search indices.
// Caller must hold m.mu.Lock.
func (m *Manager) rebuildIndicesLocked() {
	// Clear old indices.
	m.indexByIdentifier = make(map[string][]*Anko)
	m.indexBySource = make(map[string][]*Anko)
	m.indexByName = make(map[string][]*Anko)
	for _, ext := range m.fileIndex {
		meta := ext.Metadata()
		// var nameStr, idStr string
		// var sources []string

		// Read metadata under "anko" key first.
		// if ankoMeta, ok := meta["anko"].(map[any]any); ok {
		// 	if n, exists := ankoMeta["name"]; exists {
		// 		nameStr = strings.ToLower(fmt.Sprintf("%v", n))
		// 	}
		// 	if id, exists := ankoMeta["identifier"]; exists {
		// 		idStr = strings.ToLower(fmt.Sprintf("%v", id))
		// 	}
		// 	if s, exists := ankoMeta["sources"]; exists {
		// 		sources = parseSources(s)
		// 	}
		// } else {
		// 	if n, exists := meta["name"]; exists {
		// 		nameStr = strings.ToLower(fmt.Sprintf("%v", n))
		// 	}
		// 	if id, exists := meta["identifier"]; exists {
		// 		idStr = strings.ToLower(fmt.Sprintf("%v", id))
		// 	}
		// 	if s, exists := meta["sources"]; exists {
		// 		sources = parseSources(s)
		// 	}
		// }
		// if nameStr != "" {
		// 	m.indexByName[nameStr] = append(m.indexByName[nameStr], ext)
		// }
		// if idStr != "" {
		// 	m.indexByIdentifier[idStr] = append(m.indexByIdentifier[idStr], ext)
		// }
		// for _, s := range sources {
		// 	s = strings.ToLower(s)
		// 	m.indexBySource[s] = append(m.indexBySource[s], ext)
		// }

		nameStr := strings.ToLower(meta.Name)
		idStr := strings.ToLower(meta.Identifier)
		sources := meta.Sources

		if nameStr != "" {
			m.indexByName[nameStr] = append(m.indexByName[nameStr], ext)
		}
		if idStr != "" {
			m.indexByIdentifier[idStr] = append(m.indexByIdentifier[idStr], ext)
		}
		for _, s := range sources {
			s = strings.ToLower(s)
			m.indexBySource[s] = append(m.indexBySource[s], ext)
		}
	}
}

// parseSources converts the "sources" field into a slice of strings.
// func parseSources(s any) []string {
// 	var sources []string
// 	switch v := s.(type) {
// 	case []any:
// 		for _, item := range v {
// 			sources = append(sources, fmt.Sprintf("%v", item))
// 		}
// 	case []string:
// 		sources = v
// 	case string:
// 		sources = []string{v}
// 	}
// 	return sources
// }

// watchDirectory starts a filesystem watcher on the current directory (m.dir) and
// collects events. It uses a debouncing timer so that events are processed after a quiet period.
func (m *Manager) watchDirectory() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.dir == "" {
		err := fmt.Errorf("directory not set; call LoadDirectory first")
		m.mochikoLogger.Error("cannot start watcher", "error", err)
		return err
	}

	var err error
	m.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		m.mochikoLogger.Error("failed to create file watcher", "error", err)
		return fmt.Errorf("failed to create file watcher: %w", err)
	}
	if err := m.watcher.Add(m.dir); err != nil {
		m.mochikoLogger.Error("failed to add directory to watcher", "dir", m.dir, "error", err)
		return fmt.Errorf("failed to add directory %s to watcher: %w", m.dir, err)
	}

	m.hotReloadAlive = true

	go m.runDebounceLoop()

	return nil
}

// runDebounceLoop processes file events from the watcher using debouncing.
func (m *Manager) runDebounceLoop() {
	m.mochikoLogger.Info("Hot reload started", "directory", m.dir)
	var timer *time.Timer

	for {
		select {
		case event, ok := <-m.watcher.Events:
			if !ok {
				m.mochikoLogger.Warn("Watcher events channel closed")
				return
			}
			if !(filepath.Ext(event.Name) == ".yml" || filepath.Ext(event.Name) == ".yaml") {
				continue
			}
			m.mu.Lock()
			// Aggregate events per file.
			m.pendingEvents[event.Name] |= event.Op
			m.mu.Unlock()
			// Reset the timer.
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(m.debounceDuration)
		case <-func() <-chan time.Time {
			if timer != nil {
				return timer.C
			}
			// If timer is nil, block indefinitely.
			return make(chan time.Time)
		}():
			m.processPendingEvents()
		case err, ok := <-m.watcher.Errors:
			if !ok {
				m.mochikoLogger.Warn("Watcher errors channel closed")
				return
			}
			m.mochikoLogger.Error("File watcher error", "error", err)
		case <-m.hotReloadStop:
			m.mochikoLogger.Info("Hot reload stopped", "directory", m.dir)
			return
		}
	}
}

// processPendingEvents processes aggregated file events incrementally.
func (m *Manager) processPendingEvents() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for file, op := range m.pendingEvents {
		// If the file was removed, remove it from the index.
		if op&fsnotify.Remove != 0 {
			delete(m.fileIndex, file)
			m.mochikoLogger.Info("Removed anko", "file", file)
		} else if op&(fsnotify.Write|fsnotify.Create) != 0 {
			// For a write/create, attempt to reload the file.
			engine := anko.NewEngine(m.ankoLogger)
			if err := engine.LoadFile(file); err != nil {
				m.mochikoLogger.Error("Failed to reload anko", "file", file, "error", err)
				// Optionally, you might keep the old version if reload fails.
				continue
			}
			ext := &Anko{
				Engine:   engine,
				FilePath: file,
			}
			m.fileIndex[file] = ext
			m.mochikoLogger.Info("Reloaded anko", "file", file)
		}
	}
	// Clear pending events and rebuild indices.
	m.pendingEvents = make(map[string]fsnotify.Op)
	m.rebuildIndicesLocked()
	// Refresh the main anko-rui slice.
	m.ankoRui = make([]*Anko, 0, len(m.fileIndex))
	for _, ext := range m.fileIndex {
		m.ankoRui = append(m.ankoRui, ext)
	}
	m.mochikoLogger.Info("Indices rebuilt", "total_anko", len(m.ankoRui))
}

// StartHotReload initializes the hot-reload watcher for the directory holding the YAML files.
func (m *Manager) StartHotReload() error {
	if m.hotReloadAlive {
		m.mochikoLogger.Debug("Hot reload already active", "directory", m.dir)
		return nil
	}
	if err := m.watchDirectory(); err != nil {
		m.mochikoLogger.Error("failed to start hot reload", "dir", m.dir, "error", err)
		return err
	}
	return nil
}

// StopHotReload stops the hot reload watcher.
func (m *Manager) StopHotReload() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hotReloadAlive {
		m.mochikoLogger.Debug("Hot reload not active", "directory", m.dir)
		return nil
	}
	close(m.hotReloadStop)
	if m.watcher != nil {
		if err := m.watcher.Close(); err != nil {
			m.mochikoLogger.Error("failed to close watcher", "dir", m.dir, "error", err)
			return fmt.Errorf("failed to close watcher: %w", err)
		}
	}
	m.hotReloadAlive = false
	m.mochikoLogger.Info("Hot reload successfully stopped", "directory", m.dir)
	return nil
}

// ListAll returns all loaded Anko-rui.
// It will return an error if two or more anko-rui share the same identifier.
func (m *Manager) ListAll() ([]*Anko, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var duplicates []string
	for id, exts := range m.indexByIdentifier {
		if len(exts) > 1 {
			duplicates = append(duplicates, id)
		}
	}
	if len(duplicates) > 0 {
		m.mochikoLogger.Error("duplicate anko-rui found", "identifiers", duplicates)
		return nil, fmt.Errorf("duplicate anko-rui found for identifiers: %v", duplicates)
	}

	// Safe to return the slice
	return slices.Clone(m.ankoRui), nil
}

// SearchByName returns all anko-rui whose metadata "name" exactly (case-insensitive)
// matches the provided name.
func (m *Manager) SearchByName(name string) []*Anko {
	m.mu.RLock()
	defer m.mu.RUnlock()
	name = strings.ToLower(name)
	return m.indexByName[name]
}

// FuzzySearchByName performs a fuzzy search on the "name" field.
// It returns anko-rui sorted in ascending order of Levenshtein distance.
func (m *Manager) FuzzySearchByName(query string) []*Anko {
	m.mu.RLock()
	defer m.mu.RUnlock()
	query = strings.ToLower(query)
	type candidate struct {
		ext      *Anko
		distance int
	}
	var candidates []candidate
	// Check all indexed names.
	for name, exts := range m.indexByName {
		// If query is a substring, consider it a candidate.
		if strings.Contains(name, query) {
			dist := levenshtein(query, name)
			for _, ext := range exts {
				candidates = append(candidates, candidate{ext: ext, distance: dist})
			}
		}
	}
	// Optionally, filter by a maximum threshold.
	threshold := 3
	var filtered []candidate
	for _, cand := range candidates {
		if cand.distance <= threshold {
			filtered = append(filtered, cand)
		}
	}
	// Sort by distance (simple bubble sort for brevity; replace as needed).
	for i := range filtered {
		for j := i + 1; j < len(filtered); j++ {
			if filtered[j].distance < filtered[i].distance {
				filtered[i], filtered[j] = filtered[j], filtered[i]
			}
		}
	}
	// Extract the anko-rui.
	var results []*Anko
	for _, cand := range filtered {
		results = append(results, cand.ext)
	}
	return results
}

// SearchByIdentifier returns all anko-rui whose metadata "identifier" exactly (case-insensitive)
// matches the provided identifier.
func (m *Manager) SearchByIdentifier(identifier string) []*Anko {
	m.mu.RLock()
	defer m.mu.RUnlock()
	identifier = strings.ToLower(identifier)
	return m.indexByIdentifier[identifier]
}

// SearchBySource returns all anko-rui whose metadata "sources" (a list)
// includes the provided source (case-insensitive).
func (m *Manager) SearchBySource(source string) []*Anko {
	m.mu.RLock()
	defer m.mu.RUnlock()
	source = strings.ToLower(source)
	return m.indexBySource[source]
}

// LoadByIdentifier returns the anko that matches the given identifier.
// It errors if none or more than one anko is found.
func (m *Manager) LoadByIdentifier(identifier string) (*Anko, error) {
	m.mu.RLock()
	matches := m.indexByIdentifier[strings.ToLower(identifier)]
	m.mu.RUnlock()
	if len(matches) == 0 {
		err := fmt.Errorf("no anko found with identifier %s", identifier)
		m.mochikoLogger.Error("LoadByIdentifier error", "identifier", identifier, "error", err)
		return nil, err
	}
	if len(matches) > 1 {
		err := fmt.Errorf("duplicate anko-rui found with identifier %s", identifier)
		m.mochikoLogger.Error("LoadByIdentifier error", "identifier", identifier, "error", err)
		return nil, err
	}
	return matches[0], nil
}

// LoadBySource returns the anko that has the provided source in its "sources" list.
// It errors if none or more than one anko is found.
func (m *Manager) LoadBySource(source string) (*Anko, error) {
	m.mu.RLock()
	matches := m.indexBySource[strings.ToLower(source)]
	m.mu.RUnlock()
	if len(matches) == 0 {
		err := fmt.Errorf("no anko found with source %s", source)
		m.mochikoLogger.Error("LoadBySource error", "source", source, "error", err)
		return nil, err
	}
	if len(matches) > 1 {
		err := fmt.Errorf("duplicate anko-rui found with source %s", source)
		m.mochikoLogger.Error("LoadBySource error", "source", source, "error", err)
		return nil, err
	}
	return matches[0], nil
}

// levenshtein computes the Levenshtein distance between two strings.
func levenshtein(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	// Create a matrix.
	matrix := make([][]int, la+1)
	for i := range matrix {
		matrix[i] = make([]int, lb+1)
		matrix[i][0] = i
	}
	for j := 0; j <= lb; j++ {
		matrix[0][j] = j
	}
	for i := 1; i <= la; i++ {
		for j := 1; j <= lb; j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			matrix[i][j] = min(
				matrix[i-1][j]+1,      // deletion
				matrix[i][j-1]+1,      // insertion
				matrix[i-1][j-1]+cost, // substitution
			)
		}
	}
	return matrix[la][lb]
}

func min(a, b, c int) int {
	if a < b && a < c {
		return a
	}
	if b < c {
		return b
	}
	return c
}
