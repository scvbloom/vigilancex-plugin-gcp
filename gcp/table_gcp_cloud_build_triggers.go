package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/cloudbuild/v1"
)

func tableGcpCloudBuildTriggers(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_cloud_build_triggers",
		Description: "GCP Cloud Build Triggers.",
		List: &plugin.ListConfig{
			Hydrate: listCloudBuildTriggers,
		},
		Columns: []*plugin.Column{
			{
				Name:        "id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique identifier of the build trigger.",
			},
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the build trigger.",
			},
			{
				Name:        "description",
				Type:        proto.ColumnType_STRING,
				Description: "A description of the build trigger.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the build trigger was created.",
			},
			{
				Name:        "disabled",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the build trigger is disabled.",
			},
			{
				Name:        "tags",
				Type:        proto.ColumnType_JSON,
				Description: "Tags associated with the build trigger.",
			},
			{
				Name:        "trigger_template",
				Type:        proto.ColumnType_JSON,
				Description: "The trigger template configuration.",
			},
			{
				Name:        "github",
				Type:        proto.ColumnType_JSON,
				Description: "The GitHub events configuration.",
			},
			{
				Name:        "pubsub_config",
				Type:        proto.ColumnType_JSON,
				Description: "The Pub/Sub configuration.",
			},
			{
				Name:        "webhook_config",
				Type:        proto.ColumnType_JSON,
				Description: "The webhook configuration.",
			},
			{
				Name:        "bitbucket_server_trigger_config",
				Type:        proto.ColumnType_JSON,
				Description: "The Bitbucket Server trigger configuration.",
			},
			{
				Name:        "gitlab_enterprise_events_config",
				Type:        proto.ColumnType_JSON,
				Description: "The GitLab Enterprise events configuration.",
			},
			{
				Name:        "substitutions",
				Type:        proto.ColumnType_JSON,
				Description: "Substitutions for the build trigger.",
			},
			{
				Name:        "ignored_files",
				Type:        proto.ColumnType_JSON,
				Description: "Files to ignore when triggering builds.",
			},
			{
				Name:        "included_files",
				Type:        proto.ColumnType_JSON,
				Description: "Files to include when triggering builds.",
			},
			{
				Name:        "source_to_build",
				Type:        proto.ColumnType_JSON,
				Description: "The source to build configuration.",
			},
			{
				Name:        "approval_config",
				Type:        proto.ColumnType_JSON,
				Description: "The approval configuration for the build trigger.",
			},
			{
				Name:        "filter",
				Type:        proto.ColumnType_STRING,
				Description: "The filter for the build trigger.",
			},
			{
				Name:        "service_account",
				Type:        proto.ColumnType_STRING,
				Description: "The service account used for the build trigger.",
			},
			{
				Name:        "event_type",
				Type:        proto.ColumnType_STRING,
				Description: "The event type for the build trigger.",
			},
			{
				Name:        "include_build_logs",
				Type:        proto.ColumnType_STRING,
				Description: "Specifies whether to include build logs in the build trigger.",
			},
			{
				Name:        "repository_event_config",
				Type:        proto.ColumnType_JSON,
				Description: "The repository event configuration.",
			},
			// Standard Steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "tags",
				Description: ColumnDescriptionTags,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Tags"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(gcpCloudBuildTriggersTurbotData, "Akas"),
			},
		},
	}
}

//// LIST FUNCTION

func listCloudBuildTriggers(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_cloud_build_triggers.listCloudBuildTriggers", "project_error", err)
		return nil, err
	}
	project := projectData.Project

	// Get location from matrix or default to "global"
	location := d.EqualsQualString("location")
	if location == "" {
		matrixLocation := d.EqualsQualString(matrixKeyLocation)
		if matrixLocation != "" {
			location = matrixLocation
		} else {
			location = "global" // Default location
		}
	}

	// Construct the parent path
	parent := "projects/" + project + "/locations/" + location

	// Log the parent for debugging
	logger.Debug("gcp_cloud_build_triggers.listCloudBuildTriggers", "parent", parent)

	// Create Service Connection
	service, err := CloudBuildService(ctx, d)
	if err != nil {
		logger.Error("gcp_cloud_build_triggers.listCloudBuildTriggers", "connection_error", err)
		return nil, err
	}

	// List build triggers
	req := service.Projects.Locations.Triggers.List(parent)
	err = req.Pages(ctx, func(page *cloudbuild.ListBuildTriggersResponse) error {
		for _, trigger := range page.Triggers {
			d.StreamListItem(ctx, trigger)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_cloud_build_triggers.listCloudBuildTriggers", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpCloudBuildTriggersTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	trigger := d.HydrateItem.(*cloudbuild.BuildTrigger)
	akas := []string{"gcp://cloudbuild.googleapis.com/" + trigger.ResourceName}

	turbotData := map[string]interface{}{
		"Akas": akas,
	}
	return turbotData[param], nil
}
