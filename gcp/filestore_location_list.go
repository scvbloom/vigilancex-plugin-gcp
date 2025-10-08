package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// BuildFilestoreLocationList fetches and caches the list of valid locations for Filestore instances.
func BuildFilestoreLocationList(ctx context.Context, d *plugin.QueryData) []map[string]interface{} {
	// Define a cache key for Filestore locations
	locationCacheKey := "FilestoreLocation"

	// Check if the locations are already cached
	if cachedData, ok := d.ConnectionManager.Cache.Get(locationCacheKey); ok {
		plugin.Logger(ctx).Trace("BuildFilestoreLocationList: returning cached locations", cachedData.([]map[string]interface{}))
		return cachedData.([]map[string]interface{})
	}

	// Create a Filestore service connection
	service, err := FilestoreService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("BuildFilestoreLocationList: error creating Filestore service", "error", err)
		return nil
	}

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("BuildFilestoreLocationList: error fetching project details", "error", err)
		return nil
	}
	project := projectData.Project

	// Fetch the list of locations
	resp, err := service.Projects.Locations.List("projects/" + project).Do()
	if err != nil {
		plugin.Logger(ctx).Error("BuildFilestoreLocationList: error fetching locations", "error", err)
		return nil
	}

	// Build the location matrix
	matrix := make([]map[string]interface{}, len(resp.Locations))
	for i, location := range resp.Locations {
		matrix[i] = map[string]interface{}{matrixKeyLocation: location.LocationId}
	}

	// Cache the location matrix
	d.ConnectionManager.Cache.Set(locationCacheKey, matrix)

	return matrix
}
