package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/datastream/v1"
)

func tableGcpDatastreamPrivateConnection(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_datastream_private_connection",
		Description: "GCP Datastream Private Connection",
		List: &plugin.ListConfig{
			Hydrate: listDatastreamPrivateConnections,
		},
		GetMatrixItemFunc: BuildDatastreamLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "Identifier. The resource's name.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The create time of the resource.",
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The update time of the resource.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "Labels. An object containing a list of key:value pairs.",
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "Display name.",
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The state of the Private Connection.",
			},
			{
				Name:        "error",
				Type:        proto.ColumnType_JSON,
				Description: "In case of error, the details of the error in a user-friendly format.",
			},
			{
				Name:        "vpc_peering_config",
				Type:        proto.ColumnType_JSON,
				Description: "VPC Peering Config.",
			},
			{
				Name:        "satisfies_pzs",
				Type:        proto.ColumnType_BOOL,
				Description: "Reserved for future use.",
			},
			{
				Name:        "satisfies_pzi",
				Type:        proto.ColumnType_BOOL,
				Description: "Reserved for future use.",
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
				Hydrate:     gcpDatastreamPrivateConnectionTurbotData,
				Transform:   transform.FromField("Akas"),
			},

			// Standard GCP columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Hydrate:     gcpDatastreamPrivateConnectionTurbotData,
				Transform:   transform.FromField("Location"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Hydrate:     gcpDatastreamPrivateConnectionTurbotData,
				Transform:   transform.FromField("Project"),
			},
		},
	}
}

//// LIST FUNCTION

func listDatastreamPrivateConnections(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_datastream_private_connection.listDatastreamPrivateConnections", "project_error", err)
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
	logger.Debug("gcp_datastream_private_connection.listDatastreamPrivateConnections", "parent", parent)

	// Create Service Connection
	service, err := DatastreamService(ctx, d)
	if err != nil {
		logger.Error("gcp_datastream_private_connection.listDatastreamPrivateConnections", "connection_error", err)
		return nil, err
	}

	// List streams
	req := service.Projects.Locations.PrivateConnections.List(parent)
	err = req.Pages(ctx, func(page *datastream.ListPrivateConnectionsResponse) error {
		for _, stream := range page.PrivateConnections {
			d.StreamListItem(ctx, stream)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_datastream_private_connection.listDatastreamPrivateConnections", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpDatastreamPrivateConnectionTurbotData(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	privateConnection := h.Item.(*datastream.PrivateConnection)
	akas := []string{"gcp://datastream.googleapis.com/" + privateConnection.Name}
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}

	var location string
	matrixLocation := d.EqualsQualString(matrixKeyLocation)
	// Since, when the service API is disabled, matrixLocation value will be nil
	if matrixLocation != "" {
		location = matrixLocation
	}
	turbotData := map[string]interface{}{
		"Project":  projectId,
		"Location": location,
		"Akas":     akas,
	}
	return turbotData, nil
}
