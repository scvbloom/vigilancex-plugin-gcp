package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/workstations/v1"
)

func tableGcpWorkstationClusterConfiguration(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_workstation_cluster_configuration",
		Description: "GCP Workstation Cluster Configuration.",
		List: &plugin.ListConfig{
			Hydrate: listWorkstationClusterConfigurations,
		},
		GetMatrixItemFunc: BuildCloudWorkstationLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the workstation configuration.",
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The display name of the workstation configuration.",
			},
			{
				Name:        "uid",
				Type:        proto.ColumnType_STRING,
				Description: "The unique identifier of the workstation configuration.",
			},
			{
				Name:        "reconciling",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the configuration is reconciling.",
			},
			{
				Name:        "annotations",
				Type:        proto.ColumnType_JSON,
				Description: "Annotations associated with the workstation configuration.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "Labels associated with the workstation configuration.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the workstation configuration was created.",
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the workstation configuration was last updated.",
			},
			{
				Name:        "delete_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the workstation configuration was deleted.",
			},
			{
				Name:        "idle_timeout",
				Type:        proto.ColumnType_STRING,
				Description: "The idle timeout for the workstation configuration.",
			},
			{
				Name:        "running_timeout",
				Type:        proto.ColumnType_STRING,
				Description: "The running timeout for the workstation configuration.",
			},
			{
				Name:        "max_usable_workstations",
				Type:        proto.ColumnType_INT,
				Description: "The maximum number of usable workstations.",
			},
			{
				Name:        "host",
				Type:        proto.ColumnType_JSON,
				Description: "The host configuration for the workstation.",
			},
			{
				Name:        "persistent_directories",
				Type:        proto.ColumnType_JSON,
				Description: "The persistent directories associated with the workstation configuration.",
			},
			{
				Name:        "ephemeral_directories",
				Type:        proto.ColumnType_JSON,
				Description: "The ephemeral directories associated with the workstation configuration.",
			},
			{
				Name:        "container",
				Type:        proto.ColumnType_JSON,
				Description: "The container configuration for the workstation.",
			},
			{
				Name:        "encryption_key",
				Type:        proto.ColumnType_JSON,
				Description: "The encryption key used for the workstation configuration.",
			},
			{
				Name:        "readiness_checks",
				Type:        proto.ColumnType_JSON,
				Description: "The readiness checks for the workstation configuration.",
			},
			{
				Name:        "replica_zones",
				Type:        proto.ColumnType_JSON,
				Description: "The replica zones for the workstation configuration.",
			},
			{
				Name:        "degraded",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the workstation configuration is degraded.",
			},
			{
				Name:        "conditions",
				Type:        proto.ColumnType_JSON,
				Description: "The conditions associated with the workstation configuration.",
			},
			{
				Name:        "enable_audit_agent",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the audit agent is enabled.",
			},
			{
				Name:        "disable_tcp_connections",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether TCP connections are disabled.",
			},
			{
				Name:        "allowed_ports",
				Type:        proto.ColumnType_JSON,
				Description: "The allowed port ranges for the workstation configuration.",
			},
			{
				Name:        "grant_workstation_admin_role_on_create",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the workstation admin role is granted on creation.",
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
				Transform:   transform.FromP(gcpWorkstationClusterConfigurationTurbotData, "Akas"),
			},
		},
	}
}

//// LIST FUNCTION

func listWorkstationClusterConfigurations(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_workstation_cluster.listWorkstationClusters", "project_error", err)
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
	logger.Debug("gcp_workstation_cluster.listWorkstationClusters", "parent", parent)

	// Create Service Connection
	service, err := WorkstationsService(ctx, d)
	if err != nil {
		logger.Error("gcp_workstation_cluster_configuration.listWorkstationClusterConfigurations", "connection_error", err)
		return nil, err
	}

	// List workstation configurations
	req := service.Projects.Locations.WorkstationClusters.WorkstationConfigs.List(parent)
	err = req.Pages(ctx, func(page *workstations.ListWorkstationConfigsResponse) error {
		for _, config := range page.WorkstationConfigs {
			d.StreamListItem(ctx, config)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_workstation_cluster_configuration.listWorkstationClusterConfigurations", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpWorkstationClusterConfigurationTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	config := d.HydrateItem.(*workstations.WorkstationConfig)
	akas := []string{"gcp://workstations.googleapis.com/" + config.Name}

	turbotData := map[string]interface{}{
		"Akas": akas,
	}
	return turbotData[param], nil
}
