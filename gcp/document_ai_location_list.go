package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// BuildDocumentAILocationList fetches and caches the list of valid locations for DocumentAI.
func BuildDocumentAILocationList(ctx context.Context, d *plugin.QueryData) []map[string]interface{} {
	// Define a cache key for DocumentAI locations
	locationCacheKey := "DocumentAILocation"

	// Check if the locations are already cached
	if cachedData, ok := d.ConnectionManager.Cache.Get(locationCacheKey); ok {
		plugin.Logger(ctx).Trace("BuildDocumentAILocationList: returning cached locations", cachedData.([]map[string]interface{}))
		return cachedData.([]map[string]interface{})
	}

	// Create a DocumentAI service connection
	service, err := DocumentAIService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("BuildDocumentAILocationList: error creating DocumentAI service", "error", err)
		return nil
	}

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("BuildDocumentAILocationList: error fetching project details", "error", err)
		return nil
	}
	project := projectData.Project

	// Fetch the list of locations
	resp, err := service.Projects.Locations.List("projects/" + project).Do()
	if err != nil {
		plugin.Logger(ctx).Error("BuildDocumentAILocationList: error fetching locations", "error", err)
		return nil
	}

	// Build the location matrix
	matrix := make([]map[string]interface{}, len(resp.Locations))
	idx := 0
	for _, location := range resp.Locations {
		if location.LocationId == "cloud-regional" {
			// represents a global endpoint
			continue
		}
		matrix[idx] = map[string]interface{}{"location": location.LocationId}
		idx++
	}

	// Cache the location matrix
	d.ConnectionManager.Cache.Set(locationCacheKey, matrix)

	return matrix
}
