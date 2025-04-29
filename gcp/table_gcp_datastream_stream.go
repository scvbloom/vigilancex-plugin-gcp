package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/datastream/v1"
)

func tableGcpDatastreamStream(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_datastream_stream",
		Description: "GCP Datastream Stream.",
		List: &plugin.ListConfig{
			Hydrate: listDatastreamStreams,
		},
		GetMatrixItemFunc: BuildDatastreamLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the stream.",
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The display name of the stream.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the stream was created.",
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the stream was last updated.",
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The state of the stream.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "Labels associated with the stream.",
			},
			{
				Name:        "source_config",
				Type:        proto.ColumnType_JSON,
				Description: "The source configuration for the stream.",
			},
			{
				Name:        "destination_config",
				Type:        proto.ColumnType_JSON,
				Description: "The destination configuration for the stream.",
			},
			{
				Name:        "errors",
				Type:        proto.ColumnType_JSON,
				Description: "Errors on the stream.",
			},
			{
				Name:        "last_recovery_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The last recovery time of the stream.",
			},
			{
				Name:        "backfill_all",
				Type:        proto.ColumnType_JSON,
				Description: "The backfill all strategy for the stream.",
			},
			{
				Name:        "backfill_none",
				Type:        proto.ColumnType_JSON,
				Description: "The backfill none strategy for the stream.",
			},
			{
				Name:        "customer_managed_encryption_key",
				Type:        proto.ColumnType_STRING,
				Description: "The customer-managed encryption key for the stream.",
			},
			{
				Name:        "satisfies_pzs",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the stream satisfies the physical zone separation (PZS) requirement.",
			},
			{
				Name:        "satisfies_pzi",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the stream satisfies the physical zone isolation (PZI) requirement.",
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
				Transform:   transform.FromP(gcpDatastreamStreamTurbotData, "Akas"),
			},
		},
	}
}

//// LIST FUNCTION

func listDatastreamStreams(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_datastream_stream.listDatastreamStreams", "project_error", err)
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
	logger.Debug("gcp_datastream_stream.listDatastreamStreams", "parent", parent)

	// Create Service Connection
	service, err := DatastreamService(ctx, d)
	if err != nil {
		logger.Error("gcp_datastream_stream.listDatastreamStreams", "connection_error", err)
		return nil, err
	}

	// List streams
	req := service.Projects.Locations.Streams.List(parent)
	err = req.Pages(ctx, func(page *datastream.ListStreamsResponse) error {
		for _, stream := range page.Streams {
			d.StreamListItem(ctx, stream)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_datastream_stream.listDatastreamStreams", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpDatastreamStreamTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	stream := d.HydrateItem.(*datastream.Stream)
	akas := []string{"gcp://datastream.googleapis.com/" + stream.Name}

	turbotData := map[string]interface{}{
		"Akas": akas,
	}
	return turbotData[param], nil
}
