package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/datafusion/v1"
)

func tableGcpCloudDataFusionInstance(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_cloud_data_fusion_instance",
		Description: "GCP Cloud Data Fusion Instance.",
		List: &plugin.ListConfig{
			Hydrate: listCloudDataFusionInstances,
		},
		GetMatrixItemFunc: BuildCloudDataFusionLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the Data Fusion instance.",
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The display name of the Data Fusion instance.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the instance was created.",
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the instance was last updated.",
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The current state of the Data Fusion instance.",
			},
			{
				Name:        "type",
				Type:        proto.ColumnType_STRING,
				Description: "The type of the Data Fusion instance.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "Labels associated with the Data Fusion instance.",
			},
			{
				Name:        "service_endpoint",
				Type:        proto.ColumnType_STRING,
				Description: "The endpoint of the Data Fusion instance.",
			},
			{
				Name:        "zone",
				Type:        proto.ColumnType_STRING,
				Description: "The zone in which the Data Fusion instance is deployed.",
			},
			// Standard Steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("DisplayName"),
			},
			{
				Name:        "tags",
				Description: ColumnDescriptionTags,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Labels"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(gcpCloudDataFusionInstanceTurbotData, "Akas"),
			},
		},
	}
}

//// LIST FUNCTION

func listCloudDataFusionInstances(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_cloud_data_fusion_instance.listCloudDataFusionInstances", "project_error", err)
		return nil, err
	}
	project := projectData.Project

	// Get location from matrix or default to "us-central1"
	location := d.EqualsQualString("location")
	if location == "" {
		matrixLocation := d.EqualsQualString(matrixKeyLocation)
		if matrixLocation != "" {
			location = matrixLocation
		} else {
			location = "us-central1" // Default location
		}
	}

	// Construct the parent path
	parent := "projects/" + project + "/locations/" + location

	// Log the parent for debugging
	logger.Debug("gcp_cloud_data_fusion_instance.listCloudDataFusionInstances", "parent", parent)

	// Create Service Connection
	service, err := DataFusionService(ctx, d)
	if err != nil {
		logger.Error("gcp_cloud_data_fusion_instance.listCloudDataFusionInstances", "connection_error", err)
		return nil, err
	}

	// List instances
	req := service.Projects.Locations.Instances.List(parent)
	err = req.Pages(ctx, func(page *datafusion.ListInstancesResponse) error {
		for _, instance := range page.Instances {
			d.StreamListItem(ctx, instance)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_cloud_data_fusion_instance.listCloudDataFusionInstances", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpCloudDataFusionInstanceTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	instance := d.HydrateItem.(*datafusion.Instance)
	akas := []string{"gcp://datafusion.googleapis.com/" + instance.Name}

	turbotData := map[string]interface{}{
		"Akas": akas,
	}
	return turbotData[param], nil
}
