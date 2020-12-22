package provider

import (
	"context"
	"fmt"
	"path"
	"runtime"
	"sync"

	"github.com/hashicorp/terraform-plugin-go/tfprotov5"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform/providers"
	"github.com/hashicorp/terraform/tfdiags"
	"github.com/pkg/errors"
	"github.com/zclconf/go-cty/cty"
	ctyjson "github.com/zclconf/go-cty/cty/json"
	"github.com/zclconf/go-cty/cty/msgpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCClient is an inmemory implementation of the TF GRPC
type GRPCClient struct {
	NopProvider
	server *schema.GRPCProviderServer

	mu      sync.Mutex
	schemas providers.GetSchemaResponse
}

func NewGRPCClient(pv *schema.Provider) *GRPCClient {
	sv := schema.NewGRPCProviderServer(pv)
	return &GRPCClient{
		server: sv,
	}
}

func (c *GRPCClient) ReadResource(r providers.ReadResourceRequest) (resp providers.ReadResourceResponse) {
	resSchema := c.getResourceSchema(r.TypeName)
	metaSchema := c.getProviderMetaSchema()

	mp, err := msgpack.Marshal(r.PriorState, resSchema.Block.ImpliedType())
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	protoReq := &tfprotov5.ReadResourceRequest{
		TypeName:     r.TypeName,
		CurrentState: &tfprotov5.DynamicValue{MsgPack: mp},
		Private:      r.Private,
	}

	if metaSchema.Block != nil {
		metaMP, err := msgpack.Marshal(r.ProviderMeta, metaSchema.Block.ImpliedType())
		if err != nil {
			resp.Diagnostics = resp.Diagnostics.Append(err)
			return resp
		}
		protoReq.ProviderMeta = &tfprotov5.DynamicValue{MsgPack: metaMP}
	}

	protoResp, err := c.server.ReadResource(context.Background(), protoReq)
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(grpcErr(err))
		return resp
	}
	for _, d := range protoResp.Diagnostics {
		resp.Diagnostics = resp.Diagnostics.Append(errors.New(d.Summary))
	}

	state, err := decodeDynamicValue(protoResp.NewState, resSchema.Block.ImpliedType())
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}
	resp.NewState = state
	resp.Private = protoResp.Private

	return resp
}

func (c *GRPCClient) ImportResourceState(r providers.ImportResourceStateRequest) (resp providers.ImportResourceStateResponse) {
	protoReq := &tfprotov5.ImportResourceStateRequest{
		TypeName: r.TypeName,
		ID:       r.ID,
	}

	protoResp, err := c.server.ImportResourceState(context.Background(), protoReq)
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(grpcErr(err))
		return resp
	}
	for _, d := range protoResp.Diagnostics {
		resp.Diagnostics = resp.Diagnostics.Append(errors.New(d.Summary))
	}

	for _, imported := range protoResp.ImportedResources {
		resource := providers.ImportedResource{
			TypeName: imported.TypeName,
			Private:  imported.Private,
		}

		resSchema := c.getResourceSchema(resource.TypeName)
		state, err := decodeDynamicValue(imported.State, resSchema.Block.ImpliedType())
		if err != nil {
			resp.Diagnostics = resp.Diagnostics.Append(err)
			return resp
		}
		resource.State = state
		resp.ImportedResources = append(resp.ImportedResources, resource)
	}

	return resp

}

// getSchema is used internally to get the saved provider schema.  The schema
// should have already been fetched from the provider, but we have to
// synchronize access to avoid being called concurrently with GetSchema.
func (c *GRPCClient) getSchema() providers.GetSchemaResponse {
	c.mu.Lock()
	// unlock inline in case GetSchema needs to be called
	if c.schemas.Provider.Block != nil {
		c.mu.Unlock()
		return c.schemas
	}
	c.mu.Unlock()

	// the schema should have been fetched already, but give it another shot
	// just in case things are being called out of order. This may happen for
	// tests.
	schemas := c.GetSchema()
	if schemas.Diagnostics.HasErrors() {
		panic(schemas.Diagnostics.Err())
	}

	return schemas
}

// getResourceSchema is a helper to extract the schema for a resource, and
// panics if the schema is not available.
func (c *GRPCClient) getResourceSchema(name string) providers.Schema {
	schema := c.getSchema()
	resSchema, ok := schema.ResourceTypes[name]
	if !ok {
		panic("unknown resource type " + name)
	}
	return resSchema
}

// getProviderMetaSchema is a helper to extract the schema for the meta info
// defined for a provider,
func (c *GRPCClient) getProviderMetaSchema() providers.Schema {
	schema := c.getSchema()
	return schema.ProviderMeta
}

// -----
// NopProvider is an empty implementation of the providers.Interface
type NopProvider struct{}

// GetSchema returns the complete schema for the provider.
func (np *NopProvider) GetSchema() providers.GetSchemaResponse {
	return providers.GetSchemaResponse{}
}

// PrepareProviderConfig allows the provider to validate the configuration.
// The PrepareProviderConfigResponse.PreparedConfig field is unused. The
// final configuration is not stored in the state, and any modifications
// that need to be made must be made during the Configure method call.
func (np *NopProvider) PrepareProviderConfig(_ providers.PrepareProviderConfigRequest) providers.PrepareProviderConfigResponse {
	return providers.PrepareProviderConfigResponse{}
}

// ValidateResourceTypeConfig allows the provider to validate the resource
// configuration values.
func (np *NopProvider) ValidateResourceTypeConfig(_ providers.ValidateResourceTypeConfigRequest) providers.ValidateResourceTypeConfigResponse {
	return providers.ValidateResourceTypeConfigResponse{}
}

// ValidateDataSource allows the provider to validate the data source
// configuration values.
func (np *NopProvider) ValidateDataSourceConfig(_ providers.ValidateDataSourceConfigRequest) providers.ValidateDataSourceConfigResponse {
	return providers.ValidateDataSourceConfigResponse{}
}

// UpgradeResourceState is called when the state loader encounters an
// instance state whose schema version is less than the one reported by the
// currently-used version of the corresponding provider, and the upgraded
// result is used for any further processing.
func (np *NopProvider) UpgradeResourceState(_ providers.UpgradeResourceStateRequest) providers.UpgradeResourceStateResponse {
	return providers.UpgradeResourceStateResponse{}
}

// Configure configures and initialized the provider.
func (np *NopProvider) Configure(_ providers.ConfigureRequest) providers.ConfigureResponse {
	return providers.ConfigureResponse{}
}

// Stop is called when the provider should halt any in-flight actions.
//
// Stop should not block waiting for in-flight actions to complete. It
// should take any action it wants and return immediately acknowledging it
// has received the stop request. Terraform will not make any further API
// calls to the provider after Stop is called.
//
// The error returned, if non-nil, is assumed to mean that signaling the
// stop somehow failed and that the user should expect potentially waiting
// a longer period of time.
func (np *NopProvider) Stop() error {
	return nil
}

// ReadResource refreshes a resource and returns its current state.
func (np *NopProvider) ReadResource(_ providers.ReadResourceRequest) providers.ReadResourceResponse {
	return providers.ReadResourceResponse{}
}

// PlanResourceChange takes the current state and proposed state of a
// resource, and returns the planned final state.
func (np *NopProvider) PlanResourceChange(_ providers.PlanResourceChangeRequest) providers.PlanResourceChangeResponse {
	return providers.PlanResourceChangeResponse{}
}

// ApplyResourceChange takes the planned state for a resource, which may
// yet contain unknown computed values, and applies the changes returning
// the final state.
func (np *NopProvider) ApplyResourceChange(_ providers.ApplyResourceChangeRequest) providers.ApplyResourceChangeResponse {
	return providers.ApplyResourceChangeResponse{}
}

// ImportResourceState requests that the given resource be imported.
func (np *NopProvider) ImportResourceState(_ providers.ImportResourceStateRequest) providers.ImportResourceStateResponse {
	return providers.ImportResourceStateResponse{}
}

// ReadDataSource returns the data source's current state.
func (np *NopProvider) ReadDataSource(_ providers.ReadDataSourceRequest) providers.ReadDataSourceResponse {
	return providers.ReadDataSourceResponse{}
}

// Close shuts down the plugin process if applicable.
func (np *NopProvider) Close() error {
	return nil
}

// Decode a DynamicValue from either the JSON or MsgPack encoding.
func decodeDynamicValue(v *tfprotov5.DynamicValue, ty cty.Type) (cty.Value, error) {
	// always return a valid value
	var err error
	res := cty.NullVal(ty)
	if v == nil {
		return res, nil
	}

	switch {
	case len(v.MsgPack) > 0:
		res, err = msgpack.Unmarshal(v.MsgPack, ty)
	case len(v.JSON) > 0:
		res, err = ctyjson.Unmarshal(v.JSON, ty)
	}
	return res, err
}

// grpcErr extracts some known error types and formats them into better
// representations for core. This must only be called from plugin methods.
// Since we don't use RPC status errors for the plugin protocol, these do not
// contain any useful details, and we can return some text that at least
// indicates the plugin call and possible error condition.
func grpcErr(err error) (diags tfdiags.Diagnostics) {
	if err == nil {
		return
	}

	// extract the method name from the caller.
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		return diags.Append(err)
	}

	f := runtime.FuncForPC(pc)

	// Function names will contain the full import path. Take the last
	// segment, which will let users know which method was being called.
	_, requestName := path.Split(f.Name())

	// TODO: while this expands the error codes into somewhat better messages,
	// this still does not easily link the error to an actual user-recognizable
	// plugin. The grpc plugin does not know its configured name, and the
	// errors are in a list of diagnostics, making it hard for the caller to
	// annotate the returned errors.
	switch status.Code(err) {
	case codes.Unavailable:
		// This case is when the plugin has stopped running for some reason,
		// and is usually the result of a crash.
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Plugin did not respond",
			fmt.Sprintf("The plugin encountered an error, and failed to respond to the %s call. "+
				"The plugin logs may contain more details.", requestName),
		))
	case codes.Canceled:
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Request cancelled",
			fmt.Sprintf("The %s request was cancelled.", requestName),
		))
	case codes.Unimplemented:
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Unsupported plugin method",
			fmt.Sprintf("The %s method is not supported by this plugin.", requestName),
		))
	default:
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Plugin error",
			fmt.Sprintf("The plugin returned an unexpected error from %s: %v", requestName, err),
		))
	}
	return
}
