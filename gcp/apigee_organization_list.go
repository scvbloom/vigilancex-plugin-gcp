package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// BuildApigeeOrganizationList fetches and caches the list of valid organizations for Apigee.
func BuildApigeeOrganizationList(ctx context.Context, d *plugin.QueryData) []map[string]interface{} {
	// Define a cache key for Apigee organizations
	organizationCacheKey := "ApigeeOrganization"

	// Check if the organizations are already cached
	if cachedData, ok := d.ConnectionManager.Cache.Get(organizationCacheKey); ok {
		plugin.Logger(ctx).Trace("BuildApigeeOrganizationList: returning cached organizations", cachedData.([]map[string]interface{}))
		return cachedData.([]map[string]interface{})
	}

	// Create a Apigee service connection
	service, err := ApigeeService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("BuildApigeeOrganizationList: error creating Apigee service", "error", err)
		return nil
	}

	// Fetch the list of organizations
	resp, err := service.Organizations.List("organizations").Do()
	if err != nil {
		plugin.Logger(ctx).Error("BuildApigeeOrganizationList: error fetching organizations", "error", err)
		return nil
	}

	// Build the organization matrix
	matrix := make([]map[string]interface{}, len(resp.Organizations))
	for i, organization := range resp.Organizations {
		matrix[i] = map[string]interface{}{
			"organization": organization.Organization,
			"location":     organization.Location,
			"project":     organization.ProjectId,
		}
	}

	// Cache the organization matrix
	d.ConnectionManager.Cache.Set(organizationCacheKey, matrix)

	return matrix
}
