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
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the build trigger.",
			},
			{
				Name:        "id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique identifier of the build trigger.",
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
				Description: "The GitHub configuration for the build trigger.",
			},
			{
				Name:        "pubsub_config",
				Type:        proto.ColumnType_JSON,
				Description: "The Pub/Sub configuration for the build trigger.",
			},
			{
				Name:        "webhook_config",
				Type:        proto.ColumnType_JSON,
				Description: "The webhook configuration for the build trigger.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "Labels associated with the build trigger.",
			},
			// Standard Steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
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
	akas := []string{"gcp://cloudbuild.googleapis.com/" + trigger.Name}

	turbotData := map[string]interface{}{
		"Akas": akas,
	}
	return turbotData[param], nil
}
