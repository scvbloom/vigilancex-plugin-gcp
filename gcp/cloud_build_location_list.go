package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// BuildCloudBuildLocationList fetches and caches the list of valid locations for Cloud Build.
func BuildCloudBuildLocationList(ctx context.Context, d *plugin.QueryData) []map[string]interface{} {
	// Define a cache key for Cloud Build locations
	locationCacheKey := "CloudBuildLocation"

	// Check if the locations are already cached
	if cachedData, ok := d.ConnectionManager.Cache.Get(locationCacheKey); ok {
		plugin.Logger(ctx).Trace("BuildCloudBuildLocationList: returning cached locations", cachedData.([]map[string]interface{}))
		return cachedData.([]map[string]interface{})
	}
	// If not cached, fetch the locations from the API
	locations, err := fetchCloudBuildLocations(ctx, d)
	if err != nil {
		return nil
	}

	// Cache the locations for future use
	d.ConnectionManager.Cache.Set(locationCacheKey, locations)
	return locations
}

// fetchCloudBuildLocations fetches the list of valid locations for Cloud Build.
func fetchCloudBuildLocations(ctx context.Context, d *plugin.QueryData) ([]map[string]interface{}, error) {
	// Create a service connection
	service, err := CloudBuildService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("fetchCloudBuildLocations", "service_error", err)
		return nil, err
	}

	// Fetch the list of locations
	resp, err := service.Projects.Locations.List("projects/-").Do()
	if err != nil {
		plugin.Logger(ctx).Error("fetchCloudBuildLocations", "api_error", err)
		return nil, err
	}

	var locations []map[string]interface{}
	for _, location := range resp.Locations {
		locations = append(locations, map[string]interface{}{
			"name":        location.Name,
			"displayName": location.DisplayName,
			"labels":      location.Labels,
			"metadata":    location.Metadata,
		})
	}

	return locations, nil
}