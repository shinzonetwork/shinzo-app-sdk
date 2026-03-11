package signer

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/keyring"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockKeyring implements keyring.Keyring for testing.
type mockKeyring struct {
	data   map[string][]byte
	getErr error
}

func (m *mockKeyring) Get(name string) ([]byte, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	v, ok := m.data[name]
	if !ok {
		return nil, keyring.ErrNotFound
	}
	return v, nil
}

func (m *mockKeyring) Set(name string, data []byte) error {
	if m.data == nil {
		m.data = make(map[string][]byte)
	}
	m.data[name] = data
	return nil
}

func (m *mockKeyring) Delete(name string) error {
	delete(m.data, name)
	return nil
}

func (m *mockKeyring) List() ([]string, error) {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys, nil
}

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

	defraNode, _, err := defra.StartDefraInstance(testConfig, schemaApplier, nil, nil)
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

	// Flip a byte in the middle of the signature to ensure robust corruption
	sigBytes, _ := hex.DecodeString(signature)
	sigBytes[len(sigBytes)/2] ^= 0xFF
	wrongSignature := hex.EncodeToString(sigBytes)
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

	sigBytes, _ := hex.DecodeString(signature)
	sigBytes[len(sigBytes)/2] ^= 0xFF
	wrongSignature := hex.EncodeToString(sigBytes)
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

// ─── Additional coverage tests ───────────────────────────────────────────────

func TestOpenKeyring_NilConfig(t *testing.T) {
	kr, err := openKeyring(nil)
	assert.NoError(t, err)
	assert.Nil(t, kr)
}

func TestOpenKeyring_EmptySecret(t *testing.T) {
	cfg := &config.Config{}
	kr, err := openKeyring(cfg)
	assert.NoError(t, err)
	assert.Nil(t, kr)
}

func TestOpenKeyring_WithStorePath(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			KeyringSecret: "test-secret",
			Store: config.DefraStoreConfig{
				Path: tmpDir,
			},
		},
	}
	kr, err := openKeyring(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, kr)
}

func TestOpenKeyring_EmptyStorePath(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			KeyringSecret: "test-secret",
			Store:         config.DefraStoreConfig{Path: ""},
		},
	}
	kr, err := openKeyring(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, kr)
	// Clean up the "keys" directory created in CWD
	os.RemoveAll("keys")
}

func TestLoadIdentityFromFile(t *testing.T) {
	defraNode, cfg := setupTestNode(t)
	defer defraNode.Close(context.Background())

	// First, ensure identity exists via signing
	_, err := SignWithDefraKeys("test", defraNode, cfg)
	require.NoError(t, err)

	// Try loading from the store path directly
	storePath := cfg.DefraDB.Store.Path
	identity, err := loadIdentityFromFile(storePath)
	if err != nil {
		// This might fail if identity is only stored in keyring, not file
		assert.Contains(t, err.Error(), "failed to read key file")
	} else {
		assert.NotNil(t, identity)
	}
}

func TestLoadIdentityFromFile_NotFound(t *testing.T) {
	_, err := loadIdentityFromFile(t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read key file")
}

func TestLoadIdentityFromFile_InvalidHex(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, keyFileName)
	os.WriteFile(keyPath, []byte("not-valid-hex!"), 0644)

	_, err := loadIdentityFromFile(tmpDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decode key hex")
}

func TestLoadIdentityFromFile_InvalidKey(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, keyFileName)
	// Write valid hex but invalid key bytes (too short)
	os.WriteFile(keyPath, []byte("deadbeef"), 0644)

	_, err := loadIdentityFromFile(tmpDir)
	assert.Error(t, err)
}

func TestGetStorePath_WithConfig(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			Store: config.DefraStoreConfig{Path: tmpDir},
		},
	}
	path, err := getStorePath(nil, cfg)
	assert.NoError(t, err)
	assert.Equal(t, tmpDir, path)
}

func TestGetStorePath_NilConfig(t *testing.T) {
	// Without config and without the key file in common locations, should error
	_, err := getStorePath(nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not find defra_identity.key")
}

func TestGetStorePath_EmptyConfigPath(t *testing.T) {
	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			Store: config.DefraStoreConfig{Path: ""},
		},
	}
	// Without the key file in common locations, should error
	_, err := getStorePath(nil, cfg)
	assert.Error(t, err)
}

func TestGetStorePath_FindsKeyInCommonLocation(t *testing.T) {
	// Create .defra directory with key file in current directory
	os.MkdirAll(".defra", 0755)
	keyPath := filepath.Join(".defra", keyFileName)
	os.WriteFile(keyPath, []byte("dummy"), 0644)
	defer os.RemoveAll(".defra")

	path, err := getStorePath(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, ".defra", path)
}

func TestLoadIdentityFromStore_NilConfig(t *testing.T) {
	// Should fall back to file-based which will fail without the file
	_, err := loadIdentityFromStore(nil, t.TempDir())
	assert.Error(t, err)
}

func TestSignWithDefraKeys_NoStorePath(t *testing.T) {
	// Without a valid config or key file, should fail
	_, err := SignWithDefraKeys("test", nil, nil)
	assert.Error(t, err)
}

func TestSignWithP2PKeys_NoStorePath(t *testing.T) {
	_, err := SignWithP2PKeys("test", nil, nil)
	assert.Error(t, err)
}

func TestGetDefraPublicKey_NoStorePath(t *testing.T) {
	_, err := GetDefraPublicKey(nil, nil)
	assert.Error(t, err)
}

func TestGetP2PPublicKey_NoStorePath(t *testing.T) {
	_, err := GetP2PPublicKey(nil, nil)
	assert.Error(t, err)
}

// ─── Tests for uncovered branches ────────────────────────────────────────────

func TestOpenKeyring_MkdirAllFails(t *testing.T) {
	// Use a path where MkdirAll will fail (file exists as regular file, not directory)
	tmpDir := t.TempDir()
	conflictPath := filepath.Join(tmpDir, "notadir")
	// Create a regular file at the path where "keys" subdirectory would be created
	os.WriteFile(conflictPath, []byte("block"), 0644)

	cfg := &config.Config{
		DefraDB: config.DefraDBConfig{
			KeyringSecret: "test-secret",
			Store: config.DefraStoreConfig{
				Path: conflictPath, // MkdirAll(conflictPath/keys) will fail because conflictPath is a file
			},
		},
	}
	kr, err := openKeyring(cfg)
	assert.Error(t, err)
	assert.Nil(t, kr)
	assert.Contains(t, err.Error(), "failed to create keyring directory")
}

func TestLoadIdentityFromKeyring_ErrNotFound(t *testing.T) {
	kr := &mockKeyring{data: map[string][]byte{}}
	_, err := loadIdentityFromKeyring(kr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "node identity not found in keyring")
}

func TestLoadIdentityFromKeyring_GenericError(t *testing.T) {
	kr := &mockKeyring{getErr: errors.New("disk failure")}
	_, err := loadIdentityFromKeyring(kr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get identity from keyring")
	assert.Contains(t, err.Error(), "disk failure")
}

func TestLoadIdentityFromKeyring_OldFormatWithoutPrefix(t *testing.T) {
	// Generate a real secp256k1 identity so we have valid key bytes
	ident, err := identity.Generate(crypto.KeyTypeSecp256k1)
	require.NoError(t, err)
	keyBytes := ident.PrivateKey().Raw()

	// Store WITHOUT the "secp256k1:" prefix (old format)
	kr := &mockKeyring{data: map[string][]byte{
		nodeIdentityKeyName: keyBytes,
	}}
	loaded, err := loadIdentityFromKeyring(kr)
	assert.NoError(t, err)
	assert.NotNil(t, loaded)
}

func TestLoadIdentityFromKeyring_InvalidKeyBytes(t *testing.T) {
	// Store data with a valid key type prefix but garbage key bytes
	data := append([]byte(string(crypto.KeyTypeSecp256k1)+":"), []byte("bad")...)
	kr := &mockKeyring{data: map[string][]byte{
		nodeIdentityKeyName: data,
	}}
	_, err := loadIdentityFromKeyring(kr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to reconstruct private key")
}

func TestLoadIdentityFromKeyring_InvalidKeyType(t *testing.T) {
	// Use an invalid key type prefix with some bytes
	data := append([]byte("invalidtype:"), []byte("somebytes1234567890123456789012")...)
	kr := &mockKeyring{data: map[string][]byte{
		nodeIdentityKeyName: data,
	}}
	_, err := loadIdentityFromKeyring(kr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to reconstruct private key")
}

func TestLoadIdentityFromFile_ValidKeyFile(t *testing.T) {
	// Generate a real secp256k1 identity so we get valid key bytes
	ident, err := identity.Generate(crypto.KeyTypeSecp256k1)
	require.NoError(t, err)
	keyBytes := ident.PrivateKey().Raw()

	// Write the key as hex to a temp file
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, keyFileName)
	os.WriteFile(keyPath, []byte(hex.EncodeToString(keyBytes)), 0644)

	loaded, err := loadIdentityFromFile(tmpDir)
	assert.NoError(t, err)
	assert.NotNil(t, loaded)
}
