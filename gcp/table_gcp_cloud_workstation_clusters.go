package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/workstations/v1"
)

func tableGcpWorkstationCluster(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_workstation_cluster",
		Description: "GCP Workstation Cluster.",
		List: &plugin.ListConfig{
			Hydrate: listWorkstationClusters,
		},
		GetMatrixItemFunc: BuildCloudWorkstationLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the workstation cluster.",
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The display name of the workstation cluster.",
			},
			{
				Name:        "uid",
				Type:        proto.ColumnType_STRING,
				Description: "The unique identifier of the workstation cluster.",
			},
			{
				Name:        "reconciling",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the workstation cluster is reconciling.",
			},
			{
				Name:        "annotations",
				Type:        proto.ColumnType_JSON,
				Description: "Annotations associated with the workstation cluster.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "Labels associated with the workstation cluster.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the workstation cluster was created.",
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the workstation cluster was last updated.",
			},
			{
				Name:        "delete_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the workstation cluster was deleted.",
				Transform:   transform.FromField("DeleteTime").NullIfZero(),
			},
			{
				Name:        "etag",
				Type:        proto.ColumnType_STRING,
				Description: "The etag of the workstation cluster.",
			},
			{
				Name:        "network",
				Type:        proto.ColumnType_STRING,
				Description: "The VPC network of the workstation cluster.",
			},
			{
				Name:        "subnetwork",
				Type:        proto.ColumnType_STRING,
				Description: "The subnetwork of the workstation cluster.",
			},
			{
				Name:        "control_plane_ip",
				Type:        proto.ColumnType_STRING,
				Description: "The control plane IP of the workstation cluster.",
			},
			{
				Name:        "private_cluster_config",
				Type:        proto.ColumnType_JSON,
				Description: "The private cluster configuration of the workstation cluster.",
			},
			{
				Name:        "domain_config",
				Type:        proto.ColumnType_JSON,
				Description: "The domain configuration of the workstation cluster.",
			},
			{
				Name:        "degraded",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the workstation cluster is degraded.",
			},
			{
				Name:        "conditions",
				Type:        proto.ColumnType_JSON,
				Description: "The conditions of the workstation cluster.",
			},
			{
				Name:        "tags",
				Type:        proto.ColumnType_JSON,
				Description: "Tags associated with the workstation cluster.",
			},
			// Standard Steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("DisplayName"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(gcpWorkstationClusterTurbotData, "Akas"),
			},
		},
	}
}

//// LIST FUNCTION

func listWorkstationClusters(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
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
		logger.Error("gcp_workstation_cluster.listWorkstationClusters", "connection_error", err)
		return nil, err
	}

	// List workstation clusters
	req := service.Projects.Locations.WorkstationClusters.List(parent)
	err = req.Pages(ctx, func(page *workstations.ListWorkstationClustersResponse) error {
		for _, cluster := range page.WorkstationClusters {
			d.StreamListItem(ctx, cluster)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_workstation_cluster.listWorkstationClusters", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpWorkstationClusterTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	cluster := d.HydrateItem.(*workstations.WorkstationCluster)
	akas := []string{"gcp://workstations.googleapis.com/" + cluster.Name}

	turbotData := map[string]interface{}{
		"Akas": akas,
	}
	return turbotData[param], nil
}
