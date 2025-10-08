package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/workstations/v1"
)

func tableGcpCloudWorkstationConfig(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_cloud_workstation_config",
		Description: "GCP Cloud Workstation Config.",
		List: &plugin.ListConfig{
			Hydrate: listCloudWorkstationConfigs,
		},
		GetMatrixItemFunc: BuildCloudWorkstationLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the workstation.",
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The display name of the workstation.",
			},
			{
				Name:        "uid",
				Type:        proto.ColumnType_STRING,
				Description: "The unique identifier of the workstation.",
			},
			{
				Name:        "reconciling",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the workstation is reconciling.",
			},
			{
				Name:        "annotations",
				Type:        proto.ColumnType_JSON,
				Description: "Annotations associated with the workstation.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "Labels associated with the workstation.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the workstation was created.",
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the workstation was last updated.",
			},
			{
				Name:        "start_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the workstation was started.",
			},
			{
				Name:        "delete_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the workstation was deleted.",
			},
			{
				Name:        "etag",
				Type:        proto.ColumnType_STRING,
				Description: "The etag of the workstation.",
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The state of the workstation.",
			},
			{
				Name:        "host",
				Type:        proto.ColumnType_STRING,
				Description: "The host of the workstation.",
			},
			{
				Name:        "env",
				Type:        proto.ColumnType_JSON,
				Description: "Environment variables for the workstation.",
			},
			{
				Name:        "kms_key",
				Type:        proto.ColumnType_STRING,
				Description: "The KMS key used for the workstation.",
			},
			{
				Name:        "source_workstation",
				Type:        proto.ColumnType_STRING,
				Description: "The source workstation from which this workstation was created.",
			},
			{
				Name:        "runtime_host",
				Type:        proto.ColumnType_JSON,
				Description: "The runtime host configuration for the workstation.",
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
				Transform:   transform.FromP(gcpCloudWorkstationConfigTurbotData, "Akas"),
			},
		},
	}
}

//// LIST FUNCTION

func listCloudWorkstationConfigs(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_cloud_workstation_config.listCloudWorkstationConfigs", "project_error", err)
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

	// Get workstation cluster from query qualifiers
	cluster := d.EqualsQualString("cluster")
	if cluster == "" {
		logger.Error("gcp_cloud_workstation_config.listCloudWorkstationConfigs", "missing_cluster", "The cluster qualifier is required.")
		return nil, nil
	}

	// Construct the parent path
	parent := "projects/" + project + "/locations/" + location + "/workstationClusters/" + cluster + "/workstationConfigs"

	// Log the parent for debugging
	logger.Debug("gcp_cloud_workstation_config.listCloudWorkstationConfigs", "parent", parent)

	// Create Service Connection
	service, err := WorkstationsService(ctx, d)
	if err != nil {
		logger.Error("gcp_cloud_workstation_config.listCloudWorkstationConfigs", "connection_error", err)
		return nil, err
	}

	// List workstation configs
	req := service.Projects.Locations.WorkstationClusters.WorkstationConfigs.Workstations.List(parent)
	err = req.Pages(ctx, func(page *workstations.ListWorkstationsResponse) error {
		for _, workstation := range page.Workstations {
			d.StreamListItem(ctx, workstation)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_cloud_workstation_config.listCloudWorkstationConfigs", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpCloudWorkstationConfigTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	workstation := d.HydrateItem.(*workstations.Workstation)
	akas := []string{"gcp://workstations.googleapis.com/" + workstation.Name}

	turbotData := map[string]interface{}{
		"Akas": akas,
	}
	return turbotData[param], nil
}
