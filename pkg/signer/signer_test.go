package signer

import (
	"context"
	"strings"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

// containsAny checks if the string contains any of the substrings
func containsAny(s string, substrings []string) bool {
	for _, substr := range substrings {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

// setupTestNode creates a DefraDB node with keyring for testing
func setupTestNode(t *testing.T) (*node.Node, *config.Config) {
	testConfig := &config.Config{
		DefraDB: config.DefraDBConfig{
			Url:           "http://localhost:0",
			KeyringSecret: "test-secret",
			P2P: config.DefraP2PConfig{
				BootstrapPeers: []string{},
				ListenAddr:     "/ip4/0.0.0.0/tcp/0", // Use random port for test isolation
			},
			Store: config.DefraStoreConfig{
				Path: t.TempDir(),
			},
		},
		Logger: config.LoggerConfig{
			Development: true,
		},
	}

	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(`
		type User {
			name: String
		}
	`)

	defraNode, _, err := defra.StartDefraInstance(testConfig, schemaApplier)
	require.NoError(t, err)

	return defraNode, testConfig
}

func TestSignWithDefraKeys(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	message := "Hello, World!"

	signature, err := SignWithDefraKeys(message, defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, signature)
	require.Greater(t, len(signature), 0, "Signature should not be empty")
}

func TestSignWithP2PKeys(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	message := "Hello, World!"

	signature, err := SignWithP2PKeys(message, defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, signature)
	require.Greater(t, len(signature), 0, "Signature should not be empty")
}

func TestGetDefraPublicKey(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	publicKey, err := GetDefraPublicKey(defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, publicKey)
	require.Greater(t, len(publicKey), 0, "Public key should not be empty")
}

func TestGetP2PPublicKey(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	publicKey, err := GetP2PPublicKey(defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, publicKey)
	require.Greater(t, len(publicKey), 0, "Public key should not be empty")
}

func TestSignAndVerifyDefraSignature(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	message := "Test message for DefraDB signature"

	signature, err := SignWithDefraKeys(message, defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, signature)

	publicKey, err := GetDefraPublicKey(defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, publicKey)

	err = VerifyDefraSignature(publicKey, message, signature)
	require.NoError(t, err, "Signature should verify successfully")

	err = VerifyDefraSignature(publicKey, "wrong message", signature)
	require.Error(t, err, "Signature verification should fail with wrong message")
	require.Contains(t, err.Error(), "signature verification failed", "Error should indicate verification failure")

	wrongSignature := signature[:len(signature)-1] + "0"
	err = VerifyDefraSignature(publicKey, message, wrongSignature)
	require.Error(t, err, "Signature verification should fail with wrong signature")
}

func TestSignAndVerifyP2PSignature(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	message := "Test message for P2P signature"

	signature, err := SignWithP2PKeys(message, defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, signature)

	publicKey, err := GetP2PPublicKey(defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, publicKey)

	err = VerifyP2PSignature(publicKey, message, signature)
	require.NoError(t, err, "Signature should verify successfully")

	err = VerifyP2PSignature(publicKey, "wrong message", signature)
	require.Error(t, err, "Signature verification should fail with wrong message")
	require.Contains(t, err.Error(), "signature verification failed", "Error should indicate verification failure")

	wrongSignature := signature[:len(signature)-1] + "0"
	err = VerifyP2PSignature(publicKey, message, wrongSignature)
	require.Error(t, err, "Signature verification should fail with wrong signature")
}

func TestVerifyDefraSignature_InvalidInputs(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	message := "Test message"
	publicKey, err := GetDefraPublicKey(defraNode, cfg)
	require.NoError(t, err)

	// Test with invalid public key hex (use valid hex format but wrong length for secp256k1)
	// secp256k1 public keys are 33 bytes (66 hex chars) compressed, so use a shorter hex string
	// Use a valid hex signature to avoid signature decode error
	err = VerifyDefraSignature("deadbeef", message, "3006020101020101")
	require.Error(t, err)
	// The error could be either "failed to decode public key hex" or "failed to parse public key" depending on validation order
	require.True(t,
		containsAny(err.Error(), []string{"failed to decode public key hex", "failed to parse public key", "InvalidECDSAPubKey"}),
		"Should fail on invalid public key, got: %s", err.Error())

	// Test with invalid signature hex
	err = VerifyDefraSignature(publicKey, message, "invalid-hex")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decode signature hex", "Should fail on invalid signature hex")

	// Test with empty public key
	err = VerifyDefraSignature("", message, "signature")
	require.Error(t, err)

	// Test with empty signature
	err = VerifyDefraSignature(publicKey, message, "")
	require.Error(t, err)
}

func TestVerifyP2PSignature_InvalidInputs(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	message := "Test message"
	publicKey, err := GetP2PPublicKey(defraNode, cfg)
	require.NoError(t, err)

	// Test with invalid public key hex (use valid hex format but wrong length)
	err = VerifyP2PSignature("deadbeef", message, "0000000000000000000000000000000000000000000000000000000000000000")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse public key", "Should fail on invalid public key")

	// Test with invalid signature hex
	err = VerifyP2PSignature(publicKey, message, "invalid-hex")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decode signature hex", "Should fail on invalid signature hex")

	// Test with empty public key
	err = VerifyP2PSignature("", message, "signature")
	require.Error(t, err)

	// Test with empty signature
	err = VerifyP2PSignature(publicKey, message, "")
	require.Error(t, err)
}

func TestSignWithDefraKeys_Consistency(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	message := "Consistent message"

	// Sign the same message twice
	signature1, err := SignWithDefraKeys(message, defraNode, cfg)
	require.NoError(t, err)

	signature2, err := SignWithDefraKeys(message, defraNode, cfg)
	require.NoError(t, err)

	require.Equal(t, signature1, signature2)

	// Both should verify with the same public key
	publicKey, err := GetDefraPublicKey(defraNode, cfg)
	require.NoError(t, err)

	err = VerifyDefraSignature(publicKey, message, signature1)
	require.NoError(t, err)

	err = VerifyDefraSignature(publicKey, message, signature2)
	require.NoError(t, err)
}

func TestSignWithP2PKeys_Consistency(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	message := "Consistent message"

	// Sign the same message twice
	signature1, err := SignWithP2PKeys(message, defraNode, cfg)
	require.NoError(t, err)

	signature2, err := SignWithP2PKeys(message, defraNode, cfg)
	require.NoError(t, err)

	// Ed25519 signatures are deterministic, so they should be the same
	require.Equal(t, signature1, signature2, "Ed25519 signatures should be deterministic")

	// Both should verify with the same public key
	publicKey, err := GetP2PPublicKey(defraNode, cfg)
	require.NoError(t, err)

	err = VerifyP2PSignature(publicKey, message, signature1)
	require.NoError(t, err)

	err = VerifyP2PSignature(publicKey, message, signature2)
	require.NoError(t, err)
}

func TestPublicKeyConsistency(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	// Get public keys multiple times
	publicKey1, err := GetDefraPublicKey(defraNode, cfg)
	require.NoError(t, err)

	publicKey2, err := GetDefraPublicKey(defraNode, cfg)
	require.NoError(t, err)

	// Public keys should be the same
	require.Equal(t, publicKey1, publicKey2, "Public keys should be consistent")

	// Get P2P public keys multiple times
	p2pPublicKey1, err := GetP2PPublicKey(defraNode, cfg)
	require.NoError(t, err)

	p2pPublicKey2, err := GetP2PPublicKey(defraNode, cfg)
	require.NoError(t, err)

	// P2P public keys should be the same
	require.Equal(t, p2pPublicKey1, p2pPublicKey2, "P2P public keys should be consistent")
}

func TestCrossKeyVerification(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	message := "Test message"

	// Sign with Defra keys
	defraSignature, err := SignWithDefraKeys(message, defraNode, cfg)
	require.NoError(t, err)

	defraPublicKey, err := GetDefraPublicKey(defraNode, cfg)
	require.NoError(t, err)

	// Sign with P2P keys
	p2pSignature, err := SignWithP2PKeys(message, defraNode, cfg)
	require.NoError(t, err)

	p2pPublicKey, err := GetP2PPublicKey(defraNode, cfg)
	require.NoError(t, err)

	// Defra signature should NOT verify with P2P public key
	err = VerifyP2PSignature(p2pPublicKey, message, defraSignature)
	require.Error(t, err, "Defra signature should not verify with P2P public key")

	// P2P signature should NOT verify with Defra public key
	err = VerifyDefraSignature(defraPublicKey, message, p2pSignature)
	require.Error(t, err, "P2P signature should not verify with Defra public key")

	// Correct verifications should work
	err = VerifyDefraSignature(defraPublicKey, message, defraSignature)
	require.NoError(t, err)

	err = VerifyP2PSignature(p2pPublicKey, message, p2pSignature)
	require.NoError(t, err)
}

func TestSignWithDefraKeys_EmptyMessage(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	// Sign empty message
	signature, err := SignWithDefraKeys("", defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, signature)

	// Verify empty message
	publicKey, err := GetDefraPublicKey(defraNode, cfg)
	require.NoError(t, err)

	err = VerifyDefraSignature(publicKey, "", signature)
	require.NoError(t, err)
}

func TestSignWithP2PKeys_EmptyMessage(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	// Sign empty message
	signature, err := SignWithP2PKeys("", defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, signature)

	// Verify empty message
	publicKey, err := GetP2PPublicKey(defraNode, cfg)
	require.NoError(t, err)

	err = VerifyP2PSignature(publicKey, "", signature)
	require.NoError(t, err)
}

func TestSignWithDefraKeys_LongMessage(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	// Create a long message
	longMessage := make([]byte, 10000)
	for i := range longMessage {
		longMessage[i] = byte(i % 256)
	}
	message := string(longMessage)

	// Sign long message
	signature, err := SignWithDefraKeys(message, defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, signature)

	// Verify long message
	publicKey, err := GetDefraPublicKey(defraNode, cfg)
	require.NoError(t, err)

	err = VerifyDefraSignature(publicKey, message, signature)
	require.NoError(t, err)
}

func TestSignWithP2PKeys_LongMessage(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	// Create a long message
	longMessage := make([]byte, 10000)
	for i := range longMessage {
		longMessage[i] = byte(i % 256)
	}
	message := string(longMessage)

	// Sign long message
	signature, err := SignWithP2PKeys(message, defraNode, cfg)
	require.NoError(t, err)
	require.NotEmpty(t, signature)

	// Verify long message
	publicKey, err := GetP2PPublicKey(defraNode, cfg)
	require.NoError(t, err)

	err = VerifyP2PSignature(publicKey, message, signature)
	require.NoError(t, err)
}
