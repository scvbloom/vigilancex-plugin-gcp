package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/securitycenter/v1"
)

func tableGcpSecurityCommandCenterFindings(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_security_command_center_findings",
		Description: "GCP Security Command Center Findings.",
		List: &plugin.ListConfig{
			Hydrate: listSecurityCommandCenterFindings,
		},
		Columns: []*plugin.Column{
			// Finding-specific fields
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the finding.",
			},
			{
				Name:        "parent",
				Type:        proto.ColumnType_STRING,
				Description: "The parent resource of the finding.",
			},
			{
				Name:        "resource_name",
				Type:        proto.ColumnType_STRING,
				Description: "The full resource name of the Google Cloud resource associated with the finding.",
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The state of the finding.",
			},
			{
				Name:        "category",
				Type:        proto.ColumnType_STRING,
				Description: "The category of the finding.",
			},
			{
				Name:        "severity",
				Type:        proto.ColumnType_STRING,
				Description: "The severity of the finding.",
			},
			{
				Name:        "external_uri",
				Type:        proto.ColumnType_STRING,
				Description: "The URI that points to a web page outside of Security Command Center for more information about the finding.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time the finding was created.",
			},
			{
				Name:        "event_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time the event took place.",
			},
			{
				Name:        "description",
				Type:        proto.ColumnType_STRING,
				Description: "A detailed description of the finding.",
			},
			{
				Name:        "finding_class",
				Type:        proto.ColumnType_STRING,
				Description: "The class of the finding (e.g., MISCONFIGURATION).",
			},
			{
				Name:        "mute",
				Type:        proto.ColumnType_STRING,
				Description: "The mute state of the finding.",
			},
			{
				Name:        "mute_update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the mute state was last updated.",
			},
			{
				Name:        "source_properties",
				Type:        proto.ColumnType_JSON,
				Description: "Source-specific properties for the finding.",
			},
			{
				Name:        "security_marks",
				Type:        proto.ColumnType_JSON,
				Description: "User-specified security marks for the finding.",
			},
			{
				Name:        "state_change",
				Type:        proto.ColumnType_STRING,
				Description: "The state change of the finding.",
				Transform:   transform.FromField("StateChange"),
			},
			{
				Name:        "contacts",
				Type:        proto.ColumnType_JSON,
				Description: "Contact details associated with the finding.",
			},
			{
				Name:        "compliances",
				Type:        proto.ColumnType_JSON,
				Description: "Compliance standards associated with the finding.",
			},
			{
				Name:        "parent_display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The display name of the parent resource.",
			},

			// Resource-specific fields
			{
				Name:        "resource",
				Type:        proto.ColumnType_JSON,
				Description: "Information related to the Google Cloud resource associated with this finding.",
			},
			{
				Name:        "resource_display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The display name of the resource associated with the finding.",
				Transform:   transform.FromField("Resource.DisplayName"),
			},
			{
				Name:        "resource_type",
				Type:        proto.ColumnType_STRING,
				Description: "The type of the resource associated with the finding.",
				Transform:   transform.FromField("Resource.Type"),
			},
			{
				Name:        "resource_project_name",
				Type:        proto.ColumnType_STRING,
				Description: "The project name of the resource associated with the finding.",
				Transform:   transform.FromField("Resource.ProjectName"),
			},
			{
				Name:        "resource_project_display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The display name of the project associated with the resource.",
				Transform:   transform.FromField("Resource.ProjectDisplayName"),
			},
			{
				Name:        "resource_location",
				Type:        proto.ColumnType_STRING,
				Description: "The location of the resource associated with the finding.",
				Transform:   transform.FromField("Resource.Location"),
			},
			{
				Name:        "resource_path_string",
				Type:        proto.ColumnType_STRING,
				Description: "The resource path string of the resource associated with the finding.",
				Transform:   transform.FromField("Resource.ResourcePathString"),
			},

			// Standard Steampipe columns
			{
				Name:        "title",
				Description: "Title of the finding.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "akas",
				Description: "Array of globally unique identifier strings (AKAs).",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(gcpSecurityCommandCenterFindingsTurbotData, "Akas"),
			},
		},
	}
}

//// LIST FUNCTION

func listSecurityCommandCenterFindings(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_security_command_center_findings.listSecurityCommandCenterFindings", "project_error", err)
		return nil, err
	}
	project := projectData.Project

	// Construct the parent path for all sources
	parent := "projects/" + project + "/sources/-"

	// Log the parent for debugging
	logger.Debug("gcp_security_command_center_findings.listSecurityCommandCenterFindings", "parent", parent)

	// Create Service Connection
	service, err := SecurityCenterService(ctx, d)
	if err != nil {
		logger.Error("gcp_security_command_center_findings.listSecurityCommandCenterFindings", "connection_error", err)
		return nil, err
	}

	// List findings
	req := service.Projects.Sources.Findings.List(parent)
	err = req.Pages(ctx, func(page *securitycenter.ListFindingsResponse) error {
		for _, result := range page.ListFindingsResults {
			// Log the result for debugging
			logger.Debug("gcp_security_command_center_findings.listSecurityCommandCenterFindings", "finding", result.Finding)

			// Stream the Finding object
			d.StreamListItem(ctx, result.Finding)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_security_command_center_findings.listSecurityCommandCenterFindings", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpSecurityCommandCenterFindingsTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	finding := d.HydrateItem.(*securitycenter.Finding)

	akas := []string{"gcp://securitycenter.googleapis.com/" + finding.Name}

	turbotData := map[string]interface{}{
		"Akas": akas,
	}
	return turbotData[param], nil
}
