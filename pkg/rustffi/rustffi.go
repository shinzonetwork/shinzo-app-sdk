package rustffi

/*
#cgo LDFLAGS: -L${SRCDIR}/lib -lffi -ldl -lm -lpthread
#cgo darwin LDFLAGS: -lresolv -framework CoreFoundation -framework Security -framework SystemConfiguration -framework IOKit -framework DiskArbitration
#cgo linux LDFLAGS: -lresolv
#include "defra.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"sync"
	"unsafe"
)

// ErrNilNodeHandle is returned when a zero/nil node handle is passed to an FFI function.
var ErrNilNodeHandle = fmt.Errorf("defra ffi: nil node handle")

// NodeHandle is an opaque handle to a Rust DefraDB node.
type NodeHandle uintptr

// NodeOptions configures node creation.
type NodeOptions struct {
	DBPath            string // empty for in-memory
	InMemory          bool
	EnableSigning     bool
	SigningKeyType    string // "secp256k1" or "ed25519"
	SigningPrivateKey []byte // raw private key bytes, nil to auto-generate
}

var initOnce sync.Once

// Init initializes the Rust FFI library. Must be called before any other function.
// Safe to call multiple times.
func Init() {
	initOnce.Do(func() {
		C.defra_init()
	})
}

// Version returns the Rust library version string.
func Version() string {
	cstr := C.defra_version()
	if cstr == nil {
		return ""
	}
	defer C.defra_free_string(cstr)
	return C.GoString(cstr)
}

// freeString frees a C string allocated by the Rust FFI.
func freeString(ptr *C.char) {
	if ptr != nil {
		C.defra_free_string(ptr)
	}
}

// checkResult converts an FfiResult to a Go (string, error) pair.
// C.GoString copies the C string data into Go memory before the deferred
// freeString calls execute, so the pattern is safe.
func checkResult(r C.struct_FfiResult) (string, error) {
	// Copy values into Go memory before freeing C memory.
	var errMsg string
	if r.error != nil {
		errMsg = C.GoString(r.error)
		C.defra_free_string(r.error)
	}
	var val string
	if r.value != nil {
		val = C.GoString(r.value)
		C.defra_free_string(r.value)
	}

	if r.status != 0 {
		if errMsg == "" {
			errMsg = "unknown FFI error"
		}
		return "", fmt.Errorf("defra ffi: %s", errMsg)
	}
	return val, nil
}

// NewNode creates a new DefraDB node with the given options.
func NewNode(opts NodeOptions) (NodeHandle, error) {
	var cOpts C.struct_NodeInitOptions

	if opts.InMemory {
		cOpts.in_memory = 1
	}

	var cDBPath *C.char
	if opts.DBPath != "" && !opts.InMemory {
		cDBPath = C.CString(opts.DBPath)
		defer C.free(unsafe.Pointer(cDBPath))
		cOpts.db_path = cDBPath
	}

	if opts.EnableSigning {
		cOpts.enable_signing = 1
	}

	var cKeyType *C.char
	if opts.SigningKeyType != "" {
		cKeyType = C.CString(opts.SigningKeyType)
		defer C.free(unsafe.Pointer(cKeyType))
		cOpts.signing_key_type = cKeyType
	}

	if len(opts.SigningPrivateKey) > 0 {
		cKey := C.CBytes(opts.SigningPrivateKey)
		defer C.free(cKey)
		cOpts.signing_private_key = (*C.uint8_t)(cKey)
		cOpts.signing_private_key_len = C.uintptr_t(len(opts.SigningPrivateKey))
	}

	result := C.new_node(cOpts)
	defer freeString(result.error)

	if result.status != 0 {
		msg := "unknown error"
		if result.error != nil {
			msg = C.GoString(result.error)
		}
		return 0, fmt.Errorf("defra ffi: new_node: %s", msg)
	}

	return NodeHandle(result.node_ptr), nil
}

// Close closes a DefraDB node and releases resources.
func Close(node NodeHandle) error {
	if node == 0 {
		return ErrNilNodeHandle
	}
	result := C.node_close(C.uintptr_t(node))
	_, err := checkResult(result)
	return err
}

// AddSchema adds a GraphQL SDL schema to the node.
// Returns the JSON array of created CollectionVersion objects.
func AddSchema(node NodeHandle, schemaSDL string) (string, error) {
	if node == 0 {
		return "", ErrNilNodeHandle
	}
	cSchema := C.CString(schemaSDL)
	defer C.free(unsafe.Pointer(cSchema))

	result := C.add_schema(C.uintptr_t(node), nil, cSchema)
	return checkResult(result)
}

// CollectionCreate creates document(s) in a collection.
// jsonData can be a JSON object (single doc) or JSON array (batch create).
func CollectionCreate(node NodeHandle, collectionName, jsonData string) (string, error) {
	if node == 0 {
		return "", ErrNilNodeHandle
	}
	cCollection := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollection))

	cJSON := C.CString(jsonData)
	defer C.free(unsafe.Pointer(cJSON))

	result := C.collection_create(C.uintptr_t(node), nil, cCollection, cJSON)
	return checkResult(result)
}

// BeginTxn begins a new transaction. Returns the transaction ID.
func BeginTxn(node NodeHandle, readonly bool) (string, error) {
	if node == 0 {
		return "", ErrNilNodeHandle
	}
	var ro C.int32_t
	if readonly {
		ro = 1
	}

	result := C.begin_txn(C.uintptr_t(node), ro)
	// Copy values before freeing.
	var errMsg string
	if result.error != nil {
		errMsg = C.GoString(result.error)
		C.defra_free_string(result.error)
	}
	var txnID string
	if result.txn_id != nil {
		txnID = C.GoString(result.txn_id)
		C.defra_free_string(result.txn_id)
	}

	if result.status != 0 {
		if errMsg == "" {
			errMsg = "unknown error"
		}
		return "", fmt.Errorf("defra ffi: begin_txn: %s", errMsg)
	}

	if txnID == "" {
		return "", fmt.Errorf("defra ffi: begin_txn returned empty txn_id")
	}
	return txnID, nil
}

// CommitTxn commits a transaction.
func CommitTxn(node NodeHandle, txnID string) error {
	if node == 0 {
		return ErrNilNodeHandle
	}
	cTxnID := C.CString(txnID)
	defer C.free(unsafe.Pointer(cTxnID))

	result := C.commit_txn(C.uintptr_t(node), cTxnID)
	_, err := checkResult(result)
	return err
}

// RollbackTxn rolls back a transaction.
func RollbackTxn(node NodeHandle, txnID string) error {
	if node == 0 {
		return ErrNilNodeHandle
	}
	cTxnID := C.CString(txnID)
	defer C.free(unsafe.Pointer(cTxnID))

	result := C.rollback_txn(C.uintptr_t(node), cTxnID)
	_, err := checkResult(result)
	return err
}

// ExecRequest executes a GraphQL query or mutation.
// Returns the JSON response.
func ExecRequest(node NodeHandle, query string) (string, error) {
	if node == 0 {
		return "", ErrNilNodeHandle
	}
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	result := C.exec_request(C.uintptr_t(node), nil, cQuery, nil, nil)
	return checkResult(result)
}

// ExecRequestInTxn executes a GraphQL query or mutation within a transaction.
func ExecRequestInTxn(node NodeHandle, txnID, query string) (string, error) {
	if node == 0 {
		return "", ErrNilNodeHandle
	}
	cTxnID := C.CString(txnID)
	defer C.free(unsafe.Pointer(cTxnID))

	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	result := C.exec_request_in_txn(C.uintptr_t(node), cTxnID, nil, cQuery, nil, nil)
	return checkResult(result)
}

// GetCollections returns a JSON array of all collections.
func GetCollections(node NodeHandle) (string, error) {
	if node == 0 {
		return "", ErrNilNodeHandle
	}
	result := C.get_collections(C.uintptr_t(node), nil)
	return checkResult(result)
}
