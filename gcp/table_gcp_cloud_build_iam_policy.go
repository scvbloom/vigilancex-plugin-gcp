package gcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/cloudbuild/v2"
)

//// TABLE DEFINITION

func tableGcpCloudBuildConnectionIamPolicy(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_cloud_build_connection_iam_policy",
		Description: "IAM Policy for Cloud Build Connections (projects/*/locations/*/connections/*)",
		List: &plugin.ListConfig{
			Hydrate: listCloudBuildConnectionIamPolicies,
		},
		Columns: []*plugin.Column{
			// Resource identity
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "Full resource name of the connection.",
			},
			{
				Name:        "project",
				Type:        proto.ColumnType_STRING,
				Description: "GCP project ID.",
				Transform:   transform.FromField("Name").Transform(parseProject),
			},
			{
				Name:        "location",
				Type:        proto.ColumnType_STRING,
				Description: "Connection location.",
				Transform:   transform.FromField("Name").Transform(parseLocation),
			},
			{
				Name:        "connection_id",
				Type:        proto.ColumnType_STRING,
				Description: "Connection identifier.",
				Transform:   transform.FromField("Name").Transform(parseConnectionID),
			},

			// IAM Policy fields
			{
				Name:        "version",
				Type:        proto.ColumnType_INT,
				Description: "IAM policy version.",
				Transform:   transform.FromField("Policy.Version"),
			},
			{
				Name:        "etag",
				Type:        proto.ColumnType_STRING,
				Description: "IAM policy etag.",
				Transform:   transform.FromField("Policy.Etag"),
			},
			{
				Name:        "bindings",
				Type:        proto.ColumnType_JSON,
				Description: "Raw IAM bindings JSON array.",
				Transform:   transform.FromField("Policy.Bindings"),
			},

			// standard steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Hydrate:     getCloudBuildConnectionIamPolicyTurbotData,
				Transform:   transform.FromValue(),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Hydrate:     getCloudBuildConnectionIamPolicyTurbotData,
				Transform:   transform.FromValue(),
			},
		},
	}
}

type connectionIamPolicy struct {
	Name   string
	Policy *cloudbuild.Policy
}

//// LIST FUNCTION

func listCloudBuildConnectionIamPolicies(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	service, err := CloudBuildService(ctx, d)
	if err != nil {
		return nil, err
	}

	// Get project details
	projectId, err := getProject(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	// List all locations
	locations := BuildCloudBuildLocationList(ctx, d)
	if len(locations) == 0 {
		return nil, nil
	}

	for _, location := range locations {
		parent := fmt.Sprintf("projects/%s/locations/%s", project, location)
		req := service.Projects.Locations.Connections.List(parent)
		err := req.Pages(ctx, func(page *cloudbuild.ListConnectionsResponse) error {
			for _, conn := range page.Connections {
				// Fetch IAM policy for each connection
				policy, err := service.Projects.Locations.Connections.
					GetIamPolicy(conn.Name).
					Context(ctx).
					Do()
				if err != nil {
					plugin.Logger(ctx).Error("GetIamPolicy", "connection", conn.Name, "error", err)
					continue
				}
				d.StreamListItem(ctx, &connectionIamPolicy{
					Name:   conn.Name,
					Policy: policy,
				})
			}
			return nil
		})
		if err != nil {
			plugin.Logger(ctx).Error("listCloudBuildConnectionIamPolicies", "location", location, "error", err)
			continue
		}
	}

	return nil, nil
}

//// TURBOT DATA HYDRATE

func getCloudBuildConnectionIamPolicyTurbotData(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	data := h.Item.(*connectionIamPolicy)
	project, err := parseProject(ctx, &transform.TransformData{Value: data.Name})
	if err != nil {
		return nil, err
	}

	title := fmt.Sprintf("%s Cloud Build Connection IAM Policy", strings.ToUpper(project.(string)))
	akas := []string{
		fmt.Sprintf("gcp://cloudbuild.googleapis.com/v2/%s/iamPolicy", data.Name),
	}

	return map[string]interface{}{
		"Title": title,
		"Akas":  akas,
	}, nil
}

//// TRANSFORM FUNCTIONS

func parseProject(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	name, ok := d.Value.(string)
	if !ok {
		return nil, fmt.Errorf("parseProject: expected string, got %T", d.Value)
	}
	parts := strings.Split(name, "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		return parts[1], nil
	}
	return nil, fmt.Errorf("parseProject: unexpected format: %s", name)
}

func parseLocation(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	name, ok := d.Value.(string)
	if !ok {
		return nil, fmt.Errorf("parseLocation: expected string, got %T", d.Value)
	}
	parts := strings.Split(name, "/")
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] == "locations" {
			return parts[i+1], nil
		}
	}
	return nil, fmt.Errorf("parseLocation: unexpected format: %s", name)
}

func parseConnectionID(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	name, ok := d.Value.(string)
	if !ok {
		return nil, fmt.Errorf("parseConnectionID: expected string, got %T", d.Value)
	}
	parts := strings.Split(name, "/")
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] == "connections" {
			return parts[i+1], nil
		}
	}
	return nil, fmt.Errorf("parseConnectionID: unexpected format: %s", name)
}
