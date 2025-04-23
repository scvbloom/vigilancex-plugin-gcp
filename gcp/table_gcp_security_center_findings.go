package gcp

// import (
// 	"context"

// 	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
// 	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
// 	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
// 	"google.golang.org/api/securitycenter/v1"
// )

// func tableGcpSecurityCenterFindings(ctx context.Context) *plugin.Table {
// 	return &plugin.Table{
// 		Name:        "gcp_security_center_findings",
// 		Description: "GCP Security Command Center Findings.",
// 		List: &plugin.ListConfig{
// 			Hydrate: listSecurityCenterFindings,
// 			KeyColumns: []*plugin.KeyColumn{
// 				{Name: "source_name", Require: plugin.Required},
// 			},
// 		},
// 		Columns: []*plugin.Column{
// 			{
// 				Name:        "name",
// 				Type:        proto.ColumnType_STRING,
// 				Description: "The relative resource name of the finding.",
// 			},
// 			{
// 				Name:        "category",
// 				Type:        proto.ColumnType_STRING,
// 				Description: "The category of the finding.",
// 			},
// 			{
// 				Name:        "state",
// 				Type:        proto.ColumnType_STRING,
// 				Description: "The state of the finding.",
// 			},
// 			{
// 				Name:        "severity",
// 				Type:        proto.ColumnType_STRING,
// 				Description: "The severity of the finding.",
// 			},
// 			{
// 				Name:        "event_time",
// 				Type:        proto.ColumnType_TIMESTAMP,
// 				Description: "The time the event took place.",
// 			},
// 			{
// 				Name:        "create_time",
// 				Type:        proto.ColumnType_TIMESTAMP,
// 				Description: "The time the finding was created.",
// 			},
// 			{
// 				Name:        "resource_name",
// 				Type:        proto.ColumnType_STRING,
// 				Description: "The full resource name of the Google Cloud resource this finding is for.",
// 			},
// 			{
// 				Name:        "source_name",
// 				Type:        proto.ColumnType_STRING,
// 				Description: "The source name of the findings.",
// 				Transform:   transform.FromQual("source_name"),
// 			},
// 			{
// 				Name:        "external_uri",
// 				Type:        proto.ColumnType_STRING,
// 				Description: "The URI that links to a web page outside of Security Command Center for more information about the finding.",
// 			},
// 			{
// 				Name:        "labels",
// 				Type:        proto.ColumnType_JSON,
// 				Description: "User-defined key/value pairs associated with the finding.",
// 			},
// 			{
// 				Name:        "source_properties",
// 				Type:        proto.ColumnType_JSON,
// 				Description: "Source-specific properties for the finding.",
// 			},
// 			// Standard Steampipe columns
// 			{
// 				Name:        "title",
// 				Description: ColumnDescriptionTitle,
// 				Type:        proto.ColumnType_STRING,
// 				Transform:   transform.FromField("Name"),
// 			},
// 			{
// 				Name:        "tags",
// 				Description: ColumnDescriptionTags,
// 				Type:        proto.ColumnType_JSON,
// 				Transform:   transform.FromField("Labels"),
// 			},
// 			{
// 				Name:        "akas",
// 				Description: ColumnDescriptionAkas,
// 				Type:        proto.ColumnType_JSON,
// 				Transform:   transform.FromP(gcpSecurityCenterFindingTurbotData, "Akas"),
// 			},
// 		},
// 	}
// }

// //// LIST FUNCTION

// func listSecurityCenterFindings(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
// 	logger := plugin.Logger(ctx)

// 	// Get the source name from the query qualifiers
// 	sourceName := d.EqualsQualString("source_name")
// 	if sourceName == "" {
// 		logger.Error("gcp_security_center_findings.listSecurityCenterFindings", "missing_source_name", "The source_name qualifier is required.")
// 		return nil, nil
// 	}

// 	// Create Service Connection
// 	service, err := SecurityCenterService(ctx, d)
// 	if err != nil {
// 		logger.Error("gcp_security_center_findings.listSecurityCenterFindings", "connection_error", err)
// 		return nil, err
// 	}

// 	// List findings
// 	req := service.Projects.Sources.Findings.List(sourceName)
// 	err = req.Pages(ctx, func(page *securitycenter.ListFindingsResponse) error {
// 		for _, finding := range page.ListFindingsResults {
// 			d.StreamListItem(ctx, finding)

// 			// Check if context has been cancelled or if the limit has been hit
// 			if d.RowsRemaining(ctx) == 0 {
// 				return nil
// 			}
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		logger.Error("gcp_security_center_findings.listSecurityCenterFindings", "api_error", err)
// 		return nil, err
// 	}

// 	return nil, nil
// }

// //// TRANSFORM FUNCTIONS

// func gcpSecurityCenterFindingTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
// 	param := d.Param.(string)
// 	finding := d.HydrateItem.(*securitycenter.Finding)
// 	akas := []string{"gcp://securitycenter.googleapis.com/" + finding.Name}

// 	turbotData := map[string]interface{}{
// 		"Akas": akas,
// 	}
// 	return turbotData[param], nil
// }
