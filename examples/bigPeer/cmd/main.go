package main

import (
	"context"
	"fmt"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/networking"
)

func main() {
	cfg := defra.DefaultConfig
	ipAddress, err := networking.GetLANIP()
	if err != nil {
		panic(err)
	}
	listenAddress := fmt.Sprintf("/ip4/%s/tcp/9176", ipAddress)
	defraUrl := fmt.Sprintf("%s:9177", ipAddress)
	cfg.DefraDB.Store.Path = "./.defra"
	cfg.DefraDB.Url = defraUrl
	cfg.DefraDB.P2P.ListenAddr = listenAddress
	myNode, err := defra.StartDefraInstance(cfg, &defra.MockSchemaApplierThatSucceeds{})
	if err != nil {
		panic(err)
	}
	defer myNode.Close(context.Background())

	for {
		time.Sleep(1 * time.Second) // Run until cancelled
	}
}
