package server

import "sync"

// RecMu protects the recordings JSON read-modify-write cycle
// to prevent concurrent uploads from corrupting data.
// Shared between channel and router packages.
var RecMu sync.Mutex
