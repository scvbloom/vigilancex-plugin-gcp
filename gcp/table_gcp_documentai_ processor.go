package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/documentai/v1"
)

func tableGcpDocumentAIProcessor(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_document_ai_processor",
		Description: "GCP DocumentAI Processor.",
		List: &plugin.ListConfig{
			Hydrate: listDocumentAIProcessors,
		},
		GetMatrixItemFunc: BuildDocumentAILocationList,
		Columns: []*plugin.Column{
			{Name: "name", Type: proto.ColumnType_STRING, Description: "Output only. Immutable. The resource name of the processor."},
			{Name: "type", Type: proto.ColumnType_STRING, Description: "The processor type, such as: OCR_PROCESSOR, INVOICE_PROCESSOR."},
			{Name: "display_name", Type: proto.ColumnType_STRING, Description: "The display name of the processor."},
			{Name: "state", Type: proto.ColumnType_STRING, Description: "Output only. The state of the processor."},
			{Name: "default_processor_version", Type: proto.ColumnType_STRING, Description: "The default processor version."},
			{Name: "processor_version_aliases", Type: proto.ColumnType_JSON, Description: "Output only. The processor version aliases."},
			{Name: "process_endpoint", Type: proto.ColumnType_STRING, Description: "Output only. Immutable. The http endpoint that can be called to invoke processing."},
			{Name: "create_time", Type: proto.ColumnType_TIMESTAMP, Description: "The time the processor was created."},
			{Name: "kms_key_name", Type: proto.ColumnType_STRING, Description: "The KMS key used for encryption and decryption in CMEK scenarios."},
			{Name: "satisfies_pzs", Type: proto.ColumnType_BOOL, Description: "Output only. Reserved for future use."},
			{Name: "satisfies_pzi", Type: proto.ColumnType_BOOL, Description: "Output only. Reserved for future use."},

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
				Hydrate:     gcpDocumentAIProcessorTurbotData,
				Transform:   transform.FromField("Akas"),
			},

			// Standard GCP columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Hydrate:     gcpDocumentAIProcessorTurbotData,
				Transform:   transform.FromField("Location"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Hydrate:     gcpDocumentAIProcessorTurbotData,
				Transform:   transform.FromField("Project"),
			},
		},
	}
}

//// LIST FUNCTION

func listDocumentAIProcessors(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_documentai_processor.listDocumentAIProcessors", "project_error", err)
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
			location = "us" // Default location
		}
	}

	// Construct the parent path
	parent := "projects/" + project + "/locations/" + location

	// Log the parent for debugging
	logger.Debug("gcp_documentai_processor.listDocumentAIProcessors", "parent", parent)

	// Create Service Connection
	service, err := DocumentAIService(ctx, d)
	if err != nil {
		logger.Error("gcp_documentai_processor.listDocumentAIProcessors", "connection_error", err)
		return nil, err
	}

	// List processors
	req := service.Projects.Locations.Processors.List(parent)
	err = req.Pages(ctx, func(page *documentai.GoogleCloudDocumentaiV1ListProcessorsResponse) error {
		for _, processor := range page.Processors {
			d.StreamListItem(ctx, processor)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_documentai_processor.listDocumentAIProcessors", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpDocumentAIProcessorTurbotData(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	processor := h.Item.(*documentai.GoogleCloudDocumentaiV1Processor)
	akas := []string{"gcp://documentai.googleapis.com/" + processor.Name}
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
