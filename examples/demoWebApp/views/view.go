package views

import "github.com/shinzonetwork/shinzo-app-sdk/pkg/views"

// Note: in production, ShinzoHub will likely expose an API via which you can fetch views to choose from in your app. There will likely be the ability to query by name (excluding the view creator address from the name - e.g. query for "SimpleView" in this example)
// For now, we simply hardcode the views we are interested in into our app.

var simpleViewSdl string = "type SimpleView_0x2e57301b5731020a9e3cbb524bfff2b5987b5a35f62ebcc9f62ef9993704be31 {transactionHash: String}"
var viewWithLensSdl string = "type FilteredAndDecodedLogsWithLens_0xeca5fac61f15db6e84d31a17c1ef2f3f14f83e1aa8b9b0f32e4a87c23f4e5f6b {transactionHash: String}"

// View queries can be omitted as they aren't used by apps building on Shinzo
var Views []views.View = []views.View{
	views.View{
		Name: "SimpleView_0x2e57301b5731020a9e3cbb524bfff2b5987b5a35f62ebcc9f62ef9993704be31",
		Sdl:  &simpleViewSdl,
	},
	views.View{
		Name: "FilteredAndDecodedLogsWithLens_0xeca5fac61f15db6e84d31a17c1ef2f3f14f83e1aa8b9b0f32e4a87c23f4e5f6b",
		Sdl:  &viewWithLensSdl,
		// Lenses can be omitted in apps using shinzo - only the Host (and ShinzoHub for validation) needs the lenses
	},
}

