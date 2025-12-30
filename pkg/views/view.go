package views

import (
	"context"
	"fmt"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/sourcenetwork/defradb/node"
)

type View models.View

func (view *View) SubscribeTo(ctx context.Context, defraNode *node.Node) error {
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(*view.Sdl)
	err := schemaApplier.ApplySchema(ctx, defraNode)
	if err != nil {
		return fmt.Errorf("Error applying view's schema: %v", err)
	}

	err = defraNode.DB.AddP2PCollections(ctx, view.Name)
	if err != nil {
		return fmt.Errorf("Error subscribing to collection %s: %v", view.Name, err)
	}

	return nil
}
