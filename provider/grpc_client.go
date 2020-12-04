package provider

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform/providers"
)

// GRPCClient is an inmemory implementation of the TF GRPC
type GRPCClient struct {
	NopProvider
	server *schema.GRPCProviderServer
}

func NewGRPCClient(pv *schema.Provider) *GRPCClient {
	sv := schema.NewGRPCProviderServer(pv)
	return &GRPCClient{
		server: sv,
	}
}

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
