package gcp

// import (
// 	"context"

// 	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
// )

// // BuildCloudBuildLocationList fetches and caches the list of valid locations for Cloud Build.
// func BuildCloudBuildLocationList(ctx context.Context, d *plugin.QueryData) []map[string]interface{} {
// 	// Define a cache key for Cloud Build locations
// 	locationCacheKey := "CloudBuildLocation"

// 	// Check if the locations are already cached
// 	if cachedData, ok := d.ConnectionManager.Cache.Get(locationCacheKey); ok {
// 		plugin.Logger(ctx).Trace("BuildCloudBuildLocationList: returning cached locations", cachedData.([]map[string]interface{}))
// 		return cachedData.([]map[string]interface{})
// 	}

// 	// Create a Cloud Build service connection
// 	service, err := CloudBuildService(ctx, d)
// 	if err != nil {
// 		plugin.Logger(ctx).Error("BuildCloudBuildLocationList: error creating Cloud Build service", "error", err)
// 		return nil
// 	}

// 	// Get project details
// 	projectData, err := activeProject(ctx, d)
// 	if err != nil {
// 		plugin.Logger(ctx).Error("BuildCloudBuildLocationList: error fetching project details", "error", err)
// 		return nil
// 	}
// 	// project := projectData.Project

// 	// // Fetch the list of locations
// 	// //resp, err := service.Projects.Locations.List("projects/" + project).Do()
// 	// //resp, err := service.Projects.Builds.List("projects/" + project).Do()
// 	// if err != nil {
// 	// 	plugin.Logger(ctx).Error("BuildCloudBuildLocationList: error fetching locations", "error", err)
// 	// 	return nil
// 	// }

// 	// // Build the location matrix
// 	// matrix := make([]map[string]interface{}, len(resp.))
// 	// for i, location := range resp.Locations {
// 	// 	matrix[i] = map[string]interface{}{"location": location.Name}
// 	// }

// 	// // Cache the location matrix
// 	// d.ConnectionManager.Cache.Set(locationCacheKey, matrix)

// 	// return matrix
// }
