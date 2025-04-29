package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// BuildCertificateAuthorityServiceLocationList fetches and caches the list of valid locations for Certificate Authority (Private CA) service.
func BuildCertificateAuthorityServiceLocationList(ctx context.Context, d *plugin.QueryData) []map[string]interface{} {
	// Define a cache key for Certificate Authority locations
	locationCacheKey := "CertificateAuthorityServiceLocation"

	// Check if the locations are already cached
	if cachedData, ok := d.ConnectionManager.Cache.Get(locationCacheKey); ok {
		plugin.Logger(ctx).Trace("BuildCertificateAuthorityServiceLocationList: returning cached locations", cachedData.([]map[string]interface{}))
		return cachedData.([]map[string]interface{})
	}

	// Create a Certificate Authority service connection
	service, err := PrivateCAService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("BuildCertificateAuthorityServiceLocationList: error creating Private CA service", "error", err)
		return nil
	}

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("BuildCertificateAuthorityServiceLocationList: error fetching project details", "error", err)
		return nil
	}
	project := projectData.Project

	// Fetch the list of locations
	resp, err := service.Projects.Locations.List("projects/" + project).Do()
	if err != nil {
		plugin.Logger(ctx).Error("BuildCertificateAuthorityServiceLocationList: error fetching locations", "error", err)
		return nil
	}

	// Build the location matrix
	matrix := make([]map[string]interface{}, len(resp.Locations))
	for i, location := range resp.Locations {
		matrix[i] = map[string]interface{}{"location": location.LocationId}
	}

	// Cache the location matrix
	d.ConnectionManager.Cache.Set(locationCacheKey, matrix)

	return matrix
}
