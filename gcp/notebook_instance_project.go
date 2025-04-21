package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"google.golang.org/api/cloudresourcemanager/v1"
)

func BuildProjectList(ctx context.Context, d *plugin.QueryData) (map[string][]interface{}, error) {
	plugin.Logger(ctx).Trace("BuildProjectList")

	// Create the Cloud Resource Manager service client
	service, err := CloudResourceManagerService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("BuildProjectList", "connection_error", err)
		return nil, err
	}

	var projectIDs []interface{}

	// List all projects
	req := service.Projects.List()
	err = req.Pages(ctx, func(page *cloudresourcemanager.ListProjectsResponse) error {
		for _, project := range page.Projects {
			if project.LifecycleState == "ACTIVE" {
				projectIDs = append(projectIDs, project.ProjectId)
			}
		}
		return nil
	})
	if err != nil {
		plugin.Logger(ctx).Error("BuildProjectList", "api_error", err)
		return nil, err
	}

	return map[string][]interface{}{
		"project": projectIDs,
	}, nil
}
