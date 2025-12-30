package attestation

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
)

func getAttestationRecordSDL(viewName string) string {
	// Check if this is a primitive type (Block, Transaction, Log, AccessListEntry)
	primitiveTypes := []string{"Block", "Transaction", "Log", "AccessListEntry"}
	for _, primitive := range primitiveTypes {
		if viewName == primitive { // For our primitive attestation records, we use a condensed schema
			return fmt.Sprintf(`type AttestationRecord { 
				attested_doc: String
				CIDs: [String]
			}`)
		}
	}

	// If either AttestationRecord does not have unique name, we will get an error when trying to the schema (collection already exists error)
	// We want a separate collection of AttestationRecords for each View so that app clients don't receive all AttestationRecords, only those that are relevant to the collections/Views they care about - we can just append the View names as those must also be unique
	return fmt.Sprintf(`type AttestationRecord {
		attested_doc: String
		source_doc: String
		CIDs: [String]
	}`)
}

type AttestationRecord struct {
	AttestedDocId string   `json:"attested_doc"` 
	SourceDocId   string   `json:"source_doc"` 
	CIDs          []string `json:"CIDs"`
	DocType       string   `json:"docType"`
	VoteCount     int      `json:"vote_count"`
}

func AddAttestationRecordCollection(ctx context.Context, defraNode *node.Node, associatedViewName string) error {
	collectionSDL := getAttestationRecordSDL(associatedViewName)
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(collectionSDL)
	err := schemaApplier.ApplySchema(ctx, defraNode)
	if err != nil {
		return fmt.Errorf("Error adding attestation record schema %s: %w", collectionSDL, err)
	}

	attestationRecords := "AttestationRecord"
	err = defraNode.DB.AddP2PCollections(ctx, attestationRecords)
	if err != nil {
		return fmt.Errorf("Error subscribing to collection %s: %v", attestationRecords, err)
	}
	return nil
}

func GetAttestationRecords(ctx context.Context, defraNode *node.Node, associatedViewName string, viewDocIds []string) ([]AttestationRecord, error) {
	// Build a comma-separated list of quoted doc IDs for GraphQL _in filter
	quoted := make([]string, 0, len(viewDocIds))
	for _, id := range viewDocIds {
		quoted = append(quoted, fmt.Sprintf("\"%s\"", id))
	}
	inList := strings.Join(quoted, ", ")

	query := fmt.Sprintf(`query {
        AttestationRecord (filter: {attested_doc: {_in: [%s]}}) {
			docType
			vote_count
			attested_doc
			source_doc
			CIDs
		}
    }`, inList)
	records, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	if err != nil {
		return nil, fmt.Errorf("Error fetching attestation record: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("No attestation records found with query: %s", query)
	}
	return records, nil
}

// extractSchemaTypes extracts all type names from a GraphQL SDL schema
func extractSchemaTypes(schema string) ([]string, error) {
	// Find all type definitions: type TypeName { ... }
	re := regexp.MustCompile(`type\s+(\w+)\s*@?[^{]*\{`)
	matches := re.FindAllStringSubmatch(schema, -1)

	if len(matches) == 0 {
		return nil, fmt.Errorf("no type definitions found in schema")
	}

	types := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			typeName := strings.TrimSpace(match[1])
			types = append(types, typeName)
		}
	}

	return types, nil
}
