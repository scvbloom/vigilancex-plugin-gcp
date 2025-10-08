package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// BuildCloudWorkstationLocationList fetches and caches the list of valid locations for Cloud Workstations.
func BuildCloudWorkstationLocationList(ctx context.Context, d *plugin.QueryData) []map[string]interface{} {
	// Define a cache key for Cloud Workstation locations
	locationCacheKey := "CloudWorkstationLocation"

	// Check if the locations are already cached
	if cachedData, ok := d.ConnectionManager.Cache.Get(locationCacheKey); ok {
		plugin.Logger(ctx).Trace("BuildCloudWorkstationLocationList: returning cached locations", cachedData.([]map[string]interface{}))
		return cachedData.([]map[string]interface{})
	}

	// Create a Cloud Workstations service connection
	service, err := WorkstationsService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("BuildCloudWorkstationLocationList: error creating Workstations service", "error", err)
		return nil
	}

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("BuildCloudWorkstationLocationList: error fetching project details", "error", err)
		return nil
	}
	project := projectData.Project

	// Fetch the list of locations
	resp, err := service.Projects.Locations.List("projects/" + project).Do()
	if err != nil {
		plugin.Logger(ctx).Error("BuildCloudWorkstationLocationList: error fetching locations", "error", err)
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
