package gcp

import (
	"context"
	"strings"

	"cloud.google.com/go/aiplatform/apiv1/aiplatformpb"
	notebooks "cloud.google.com/go/notebooks/apiv1"
	"cloud.google.com/go/notebooks/apiv1/notebookspb"
	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/iterator"
)

func tableGcpVertexAINotebookInstance(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_vertex_ai_notebook_instance",
		Description: "GCP Vertex AI Notebook Instance",
		Get: &plugin.GetConfig{
			KeyColumns:        plugin.SingleColumn("name"),
			Hydrate:           getNotebookInstance,
			ShouldIgnoreError: isIgnorableError([]string{"Unauthenticated", "Unimplemented", "InvalidArgument"}),
		},
		List: &plugin.ListConfig{
			Hydrate:           listNotebookInstances,
			ShouldIgnoreError: isIgnorableError([]string{"Unauthenticated", "Unimplemented", "InvalidArgument"}),
		},
		GetMatrixItemFunc: BuildVertexAILocationListByClientType("Notebook"),
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The resource name of the Notebook instance.",
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The display name of the Notebook instance.",
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The current state of the Notebook instance.",
			},
			{
				Name:        "machine_type",
				Type:        proto.ColumnType_STRING,
				Description: "The Compute Engine machine type of the instance.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("CreateTime").Transform(convertTimestamppbAsTime),
				Description: "Timestamp when the Notebook instance was created.",
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("UpdateTime").Transform(convertTimestamppbAsTime),
				Description: "Timestamp when the Notebook instance was last updated.",
			},
			{
				Name:        "network",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the VPC network the instance is in.",
			},
			{
				Name:        "subnet",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the subnet the instance is in.",
			},
			{
				Name:        "service_account",
				Type:        proto.ColumnType_STRING,
				Description: "The service account associated with the instance.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "The labels associated with the Notebook instance.",
			},
			{
				Name:        "metadata",
				Type:        proto.ColumnType_JSON,
				Description: "The metadata associated with the Notebook instance.",
			},
			{
				Name:        "tags",
				Type:        proto.ColumnType_JSON,
				Description: "The network tags associated with the instance.",
			},
			// Standard GCP columns
			{
				Name:        "location",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(gcpNotebookInstance, "Location"),
				Description: ColumnDescriptionLocation,
			},
			{
				Name:        "project",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getProject,
				Transform:   transform.FromValue(),
				Description: ColumnDescriptionProject,
			},
		},
	}
}

func listNotebookInstances(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	region := d.EqualsQualString("location")
	var location string
	matrixLocation := d.EqualsQualString(matrixKeyLocation)
	// Since, when the service API is disabled, matrixLocation value will be nil
	if matrixLocation != "" {
		location = matrixLocation
	}

	// Minimize API call as per given location
	if region != "" && region != location {
		logger.Warn("gcp_vertex_ai_notebook_instance.listAIPlatformModels", "location", region, "matrixLocation", location)
		return nil, nil
	}

	// Get project details

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_vertex_ai_notebook_instance.listAIPlatformModels", "cache_error", err)
		return nil, err
	}

	project := projectId.(string)

	// Page size should be in range of [0, 100].
	pageSize := types.Int64(100)
	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < *pageSize {
			pageSize = limit
		}
	}

	// Create Service Connection
	service, err := AIService(ctx, d, "Model")
	if err != nil {
		logger.Error("gcp_vertex_ai_notebook_instance.listAIPlatformModels", "connection_error", err)
		return nil, err
	}

	req := &aiplatformpb.ListModelsRequest{
		Parent:   "projects/" + project + "/locations/" + location,
		PageSize: int32(*pageSize),
	}

	it := service.Model.ListModels(ctx, req)

	for {
		model, err := it.Next()
		if err != nil {
			if strings.Contains(err.Error(), "404") {
				return nil, nil
			}
			if err == iterator.Done {
				break
			}
			logger.Error("gcp_vertex_ai_notebook_instance.listAIPlatformModels", err)
			return nil, err
		}

		d.StreamListItem(ctx, model)

		if d.RowsRemaining(ctx) == 0 {
			break
		}
	}

	return nil, nil
}

func getNotebookInstance(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	matrixLocation := d.EqualsQualString(matrixKeyLocation)

	name := d.EqualsQualString("name")
	splitName := strings.Split(name, "/")

	if len(name) > 3 && splitName[3] != matrixLocation {
		return nil, nil
	}

	client, err := notebooks.NewNotebookClient(ctx)
	if err != nil {
		logger.Error("gcp_vertex_ai_notebook_instance.getNotebookInstance", "client_creation_error", err)
		return nil, err
	}
	defer client.Close()

	req := &notebookspb.GetInstanceRequest{
		Name: name,
	}

	result, err := client.GetInstance(ctx, req)
	if err != nil {
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "NotFound") {
			return nil, nil
		}
		logger.Error("gcp_vertex_ai_notebook_instance.getNotebookInstance", "api_error", err)
		return nil, err
	}

	return result, nil
}

func gcpNotebookInstance(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	AIData := d.HydrateItem.(*notebookspb.Instance)
	akas := []string{"gcp://notebooks.googleapis.com/" + AIData.Name}

	turbotData := map[string]interface{}{
		"Location": strings.Split(AIData.Name, "/")[3],
		"Akas":     akas,
	}
	return turbotData[param], nil
}
