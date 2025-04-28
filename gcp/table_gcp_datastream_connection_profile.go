package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/datastream/v1"
)

func tableGcpDatastreamConnectionProfile(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_datastream_connection_profile",
		Description: "GCP Datastream Connection Profile",
		List: &plugin.ListConfig{
			Hydrate: listDatastreamConnectionProfiles,
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
				Name:        "satisfies_pzs",
				Type:        proto.ColumnType_BOOL,
				Description: "Reserved for future use.",
			},
			{
				Name:        "satisfies_pzi",
				Type:        proto.ColumnType_BOOL,
				Description: "Reserved for future use.",
			},
			{
				Name:        "oracle_profile",
				Type:        proto.ColumnType_JSON,
				Description: "Oracle ConnectionProfile configuration.",
			},
			{
				Name:        "gcs_profile",
				Type:        proto.ColumnType_JSON,
				Description: "Cloud Storage ConnectionProfile configuration.",
			},
			{
				Name:        "mysql_profile",
				Type:        proto.ColumnType_JSON,
				Description: "MySQL ConnectionProfile configuration.",
			},
			{
				Name:        "bigquery_profile",
				Type:        proto.ColumnType_JSON,
				Description: "BigQuery Connection Profile configuration.",
			},
			{
				Name:        "postgresql_profile",
				Type:        proto.ColumnType_JSON,
				Description: "PostgreSQL Connection Profile configuration.",
			},
			{
				Name:        "sql_server_profile",
				Type:        proto.ColumnType_JSON,
				Description: "SQLServer Connection Profile configuration.",
			},
			{
				Name:        "salesforce_profile",
				Type:        proto.ColumnType_JSON,
				Description: "Salesforce Connection Profile configuration.",
			},
			{
				Name:        "static_service_ip_connectivity",
				Type:        proto.ColumnType_JSON,
				Description: "Static Service IP connectivity.",
			},
			{
				Name:        "forward_ssh_connectivity",
				Type:        proto.ColumnType_JSON,
				Description: "Forward SSH tunnel connectivity.",
			},
			{
				Name:        "private_connectivity",
				Type:        proto.ColumnType_JSON,
				Description: "Private connectivity.",
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
				Hydrate:     gcpDatastreamConnectionProfileTurbotData,
				Transform:   transform.FromField("Akas"),
			},

			// Standard GCP columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Hydrate:     gcpDatastreamConnectionProfileTurbotData,
				Transform:   transform.FromField("Location"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Hydrate:     gcpDatastreamConnectionProfileTurbotData,
				Transform:   transform.FromField("Project"),
			},
		},
	}
}

//// LIST FUNCTION

func listDatastreamConnectionProfiles(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_datastream_connection_profile.listDatastreamConnectionProfiles", "project_error", err)
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
	logger.Debug("gcp_datastream_connection_profile.listDatastreamConnectionProfiles", "parent", parent)

	// Create Service Connection
	service, err := DatastreamService(ctx, d)
	if err != nil {
		logger.Error("gcp_datastream_connection_profile.listDatastreamConnectionProfiles", "connection_error", err)
		return nil, err
	}

	// List streams
	req := service.Projects.Locations.ConnectionProfiles.List(parent)
	err = req.Pages(ctx, func(page *datastream.ListConnectionProfilesResponse) error {
		for _, stream := range page.ConnectionProfiles {
			d.StreamListItem(ctx, stream)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_datastream_connection_profile.listDatastreamConnectionProfiles", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpDatastreamConnectionProfileTurbotData(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	connectionProfile := h.Item.(*datastream.ConnectionProfile)
	akas := []string{"gcp://datastream.googleapis.com/" + connectionProfile.Name}
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
