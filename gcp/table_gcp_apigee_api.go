package gcp

import (
	"context"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/apigee/v1"
)

func tableGcpApigeeApi(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_apigee_api",
		Description: "GCP Dataapi Stream.",
		List: &plugin.ListConfig{
			Hydrate: listApigeeApis,
		},
		GetMatrixItemFunc: BuildApigeeOrganizationList,
		Columns: []*plugin.Column{
			{Name: "name", Type: proto.ColumnType_STRING, Description: "Output only. Name of the API proxy."},
			{Name: "meta_data", Type: proto.ColumnType_JSON, Description: "Output only. Metadata describing the API proxy."},
			{Name: "revision", Type: proto.ColumnType_JSON, Description: "Output only. List of revisions defined for the API proxy."},
			{Name: "latest_revision_id", Type: proto.ColumnType_STRING, Description: "Output only. The id of the most recently created revision for this API proxy."},
			{Name: "labels", Type: proto.ColumnType_JSON, Description: "User labels applied to this API Proxy."},
			{Name: "read_only", Type: proto.ColumnType_BOOL, Description: "Output only. Whether this proxy is read-only."},
			{Name: "api_proxy_type", Type: proto.ColumnType_STRING, Description: "Output only. The type of the API proxy."},
			{Name: "space", Type: proto.ColumnType_STRING, Description: "Optional. The id of the space this proxy is associated with."},

			// Steampipe standard columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(getApigeeApiDisplayName),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Hydrate:     gcpApigeeApiTurbotData,
				Transform:   transform.FromField("Akas"),
			},

			// Standard GCP columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Hydrate:     gcpApigeeApiTurbotData,
				Transform:   transform.FromField("Location"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Hydrate:     gcpApigeeApiTurbotData,
				Transform:   transform.FromField("Project"),
			},
		},
	}
}

//// LIST FUNCTION

func listApigeeApis(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get organization from matrix or default to "us-central1"
	organization := d.EqualsQualString("organization")
	if organization == "" {
		matrixLocation := d.EqualsQualString(matrixKeyLocation)
		if matrixLocation != "" {
			organization = matrixLocation
		} else {
			organization = "<?>" // TODO : Default organization?
		}
	}

	// Construct the parent path
	parent := "organizations/" + organization

	// Log the parent for debugging
	logger.Debug("gcp_apigee_api.listApigeeApis", "parent", parent)

	// Create Service Connection
	service, err := ApigeeService(ctx, d)
	if err != nil {
		logger.Error("gcp_apigee_api.listApigeeApis", "connection_error", err)
		return nil, err
	}

	// List apis
	req := service.Organizations.Apis.List(parent)
	resp, err := req.Do()
	if err != nil {
		logger.Error("gcp_apigee_api.listApigeeApis", "api_error", err)
		return nil, err
	}
	for _, api := range resp.Proxies {
		d.StreamListItem(ctx, api)
		// Stop if the context is cancelled
		if d.RowsRemaining(ctx) == 0 {
			return nil, nil
		}
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpApigeeApiTurbotData(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	api := h.Item.(*apigee.GoogleCloudApigeeV1ApiProxy)
	matrixOrganization := d.EqualsQualString("organization")
	matrixLocation := d.EqualsQualString(matrixKeyLocation)
	matrixProject := d.EqualsQualString("project")

	// Build resource aka
	akas := []string{"gcp://apigee.googleapis.com/" + api.Name}

	// Mapping all turbot defined properties
	turbotData := map[string]interface{}{
		"Akas":  akas,
		"Organization": matrixOrganization,
		"Location":     matrixLocation,
		"Project":      matrixProject,
	}

	return turbotData, nil
}

//// TRANSFORM FUNCTION

func getApigeeApiDisplayName(ctx context.Context, h *transform.TransformData) (interface{}, error) {
	displayName := ""
	if h.HydrateItem != nil {
		data := h.HydrateItem.(*apigee.GoogleCloudApigeeV1ApiProxy)
		displayName = strings.Split(data.Name, "/")[3]
	}

	return displayName, nil
}
