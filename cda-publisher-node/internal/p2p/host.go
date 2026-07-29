package p2p

import (
	"log"
)

type Host struct {
	ID        string
	Addresses []string
}

func NewHost(id string, addresses []string) *Host {
	h := &Host{
		ID:        id,
		Addresses: addresses,
	}
	log.Printf("[P2P] Initialized Host with ID %s listening on %v", h.ID, h.Addresses)
	return h
}
