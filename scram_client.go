package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	"github.com/xdg-go/scram"
)

// HashGeneratorFcn is a function that returns a hash.Hash
type HashGeneratorFcn func() hash.Hash

// SHA256 is a HashGeneratorFcn that returns a sha256 hash.Hash
var SHA256 HashGeneratorFcn = sha256.New

// SHA512 is a HashGeneratorFcn that returns a sha512 hash.Hash
var SHA512 HashGeneratorFcn = sha512.New

// XDGSCRAMClient implements the sarama.SCRAMClient interface
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	HashGeneratorFcn scram.HashGeneratorFcn
}

// Begin starts a new SCRAM conversation
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step continues the SCRAM conversation
func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

// Done returns true if the SCRAM conversation is complete
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
